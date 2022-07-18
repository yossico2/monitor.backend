import abc
import json
import socketio
import threading
import eventlet
from datetime import datetime
from typing import Generic, List
from elasticsearch.client import Elasticsearch
from elasticsearch_dsl import Search
from pydantic.json import pydantic_encoder

import config
from state_sql import StateSQL
import utils
from model import T, PowerBlock, States
from redis_cache import RedisCache, Bucket, BUCKET_TIMEDELTA

STREAM_INTERVAL = config.PERIOD_MS/1000

# ----------------------------------------------------------------------------------
# Cache utils
# ----------------------------------------------------------------------------------


class GenericFetcher(abc.ABC, Generic[T]):
    '''
    Generic cache fetcher.

    Fetch and return the list of records between start-date (inclusive) and end-date (exclusive).

    The fetcher uses cached records if available. 

    If not found in cache, records are fetched from the upstream (es) and the cache is updated.

    Notice the following things.
    - We define the GenericFetcher as an abstract base class (inherited from ABC)
      and defined some methods as abstract. It's an extra saftey net. The interpreter
      prohibits instantiation of abstract classes or their subclasses without all
      abstract methods defined. Read more about abstract base
      classes at https://docs.python.org/3/library/abc.html
    - On top of this, we define this class as "generic." In our case, it means that
      subclasses can specify with types methods fetch() and get_values_from_upstream()
      will return. Read more: https://mypy.readthedocs.io/en/stable/generics.html
    '''

    @abc.abstractmethod
    def get_redis_cache(self) -> RedisCache:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_values_from_upstream(self, bucket: Bucket) -> List[T]:
        raise NotImplementedError()

    def _is_bucket_full(self, bucket: Bucket, fetched_items: List[T]):
        return len(fetched_items) > 0 and fetched_items[-1].timestamp == (bucket.end - config.PERIOD_MS)

    def fetch(self, start_date_ms: int, end_date_ms: int) -> List[T]:

        if end_date_ms < start_date_ms:
            raise ValueError(f"end-date must be greater than start-date")

        # initialize a list of buckets in range
        buckets = self._init_buckets(start_date_ms, end_date_ms)

        data_items: list[T] = []
        redis_cache = self.get_redis_cache()
        for bucket in buckets:

            # check if cache contains bucket
            cached_items: list[T] = redis_cache.get_bucket(bucket)

            # skip get from upstream only when a whole bucket cached
            # (partial cached buckets will be fetched again when required)
            if self._is_bucket_full(bucket, cached_items):
                # cache hit
                data_items += cached_items
                continue

            # cache miss (fetch bucket from the upstream)
            fetched_items = self.get_values_from_upstream(bucket)

            # cache items
            redis_cache.set_items(fetched_items)

            data_items += fetched_items

        # return only items in range
        items_in_range = [
            item for item in data_items if start_date_ms <= item.timestamp < end_date_ms
        ]

        return items_in_range

    def _init_buckets(self, start_date_ms: int, end_date_ms: int) -> List[Bucket]:
        '''
        return a list of buckets in a date range.
        '''
        buckets: list[Bucket] = []

        bucket = Bucket(start=start_date_ms)

        while True:
            buckets.append(bucket)
            bucket = bucket.next()
            if bucket.end >= end_date_ms:
                break

        return buckets

# ----------------------------------------------------------------------------------
# PowerBlock Fetcher
# ----------------------------------------------------------------------------------


class PowerBlockFetcher(GenericFetcher[PowerBlock]):
    '''
    PowerBlock fetcher.

    Fetch and return the list of PowerBlock records between start-date (inclusive) and end-date (exclusive).

    Usage example:
        fetcher = PowerBlockFetcher(redis_cache, es, es_index)
        power_blocks = fetcher.fetch(start-date, end-date)
    '''

    def __init__(self,
                 redis_cache: RedisCache,
                 es: Elasticsearch,
                 pb_index: str,
                 state_SQL: StateSQL):
        self.es = es
        self.redis_cache = redis_cache
        self.pb_index = pb_index
        self.state_SQL = state_SQL

    def get_redis_cache(self) -> RedisCache:
        return self.redis_cache

    def get_values_from_upstream(self, bucket: Bucket) -> List[PowerBlock]:
        '''
        Pull from ElasticSearch data items in bucket range, convert them to model 
        instances, and return them as a sorted list by timestamp.
        '''

        # ElasticSearch query
        s = Search(using=self.es, index=self.pb_index).filter(
            'range',
            timestamp={
                'gte': bucket.start,
                'lt': bucket.end,
            },
        )

        # We use scan() instead of execute() to fetch all the records, and wrap it with a
        # list() to pull everything in memory. As long as we fetch data in buckets of
        # limited sizes, memory is not a problem.
        records = list(s.params(preserve_order=True).scan())

        # convert to model objects
        blocks = [
            PowerBlock(
                timestamp=record.timestamp,
                frequency=record.frequency,
                power=record.power,
                state=States.init.value
            )
            for record in records
        ]

        # fetch state for all blocks
        self.fetch_states(blocks)

        return blocks

    def fetch_states(self, blocks: List[PowerBlock]):
        '''
        fetch state for all blocks
        '''
        if len(blocks) == 0:
            return

        if len(blocks) == 1:
            state = self.state_SQL.get_state(timestamp=blocks[0].timestamp)
            blocks[0].state = state
            return

        # multiple state are returned as list of tuples [(timestamp, state),...]
        start = blocks[0].timestamp
        end = blocks[-1].timestamp
        states = self.state_SQL.get_states(start=start, end=end)
        lookup = dict(states)
        for block in blocks:
            state = lookup.get(block.timestamp)
            block.state = state if state else States.init.value

# ----------------------------------------------------------------------------------
# Streamer
# ----------------------------------------------------------------------------------


class Streamer:
    def __init__(
        self,
        sid: str,
        sio: socketio.Server,
        redis_cache: RedisCache,
        state_SQL: StateSQL
    ) -> None:

        # SocketIO
        self.sid = sid  # sio client socket id
        self.sio = sio

        self.streaming = False
        self.paused = False

        self.streaming_lock = threading.Lock()

        # ElasticSearch (upstream)
        self.es = Elasticsearch(hosts=[config.ES_HOST])

        self.fetcher = PowerBlockFetcher(
            redis_cache=redis_cache,
            es=self.es,
            pb_index=config.ES_POWER_BLOCKS_INDEX,
            state_SQL=state_SQL
        )

    def _sleep(self, duration):
        eventlet.sleep(duration)

    def fetch(self, start_date: datetime, end_date: datetime):
        '''
        fetch events between (start_date, end_date)
        '''
        start_date_ms = utils.datetime_to_ms_since_epoch(start_date)
        end_date_ms = utils.datetime_to_ms_since_epoch(end_date)

        pb_array = self.fetcher.fetch(start_date_ms, end_date_ms)

        pb_array_json = json.dumps(pb_array, default=pydantic_encoder)
        self.sio.emit('power-blocks', data=pb_array_json, to=self.sid)

    def _upstream_data_after_date(self, date_ms: int):
        # get data item with timestamp greater than dt
        s = Search(using=self.es, index=config.ES_POWER_BLOCKS_INDEX).filter(
            "range", timestamp={"gte": date_ms},
        ).sort('timestamp')

        s = s[0:1]  # we only need one

        result = list(s.execute())
        return result

    def _upstream_has_data_after_date(self, date_ms: int):
        result = self._upstream_data_after_date(date_ms)
        if len(result) > 0:
            return True

        # NO data available at upstream after dt
        return False

    def stream(self, start_date: datetime):
        '''
        start streamimg data points (PowerBlock objects) starting from start_date.
        '''
        # fail if already streaming
        with self.streaming_lock:
            if self.streaming:
                raise RuntimeError('already streaming!')
            self.streaming = True
        self.sio.start_background_task(self.stream_background_task, start_date)

    def stream_background_task(self, start_date: datetime):

        # fetch 1 sec
        start_date_ms = utils.datetime_to_ms_since_epoch(start_date)
        end_date_ms = start_date_ms + BUCKET_TIMEDELTA

        # start streaming
        while True:
            with self.streaming_lock:
                if not self.streaming:
                    return  # stop streaming

            if self.paused:
                self._sleep(STREAM_INTERVAL)
                continue  # pause

            #  fetch items
            pb_array: PowerBlock = self.fetcher.fetch(
                start_date_ms, end_date_ms)
            if len(pb_array) == 0:
                # either no data in range or data not available yet in upstream
                # (e.g: start-date/end-date in the future)
                result = self._upstream_data_after_date(end_date_ms)
                if len(result) == 0:
                    # no data >= end_date_ms available in upstream
                    self._sleep(STREAM_INTERVAL)
                    continue

                # advance range (skip data gap)
                # continue from where we have data
                start_date_ms = result[0].timestamp
                end_date_ms = start_date_ms + BUCKET_TIMEDELTA
                continue

            # stream power_blocks
            for pb in pb_array:

                with self.streaming_lock:
                    if not self.streaming:
                        return  # stop streaming

                while self.paused:
                    self._sleep(STREAM_INTERVAL)
                    continue  # pause

                pb_json = json.dumps([pb], default=pydantic_encoder)
                self.sio.emit('power-blocks', data=pb_json, to=self.sid)

                #  print timestamp of power_block
                if config.DEBUG_STREAMER:
                    print(f'emit power-block (timestamp: {pb.timestamp})')

                self._sleep(STREAM_INTERVAL)

            # advance range (skip no data)
            start_date_ms = pb_array[-1].timestamp + config.PERIOD_MS
            end_date_ms = start_date_ms + BUCKET_TIMEDELTA

    def pause(self):
        '''
        pause streamimg
        '''
        with self.streaming_lock:
            if self.streaming:
                self.paused = True

    def play(self):
        '''
        play
        '''
        with self.streaming_lock:
            if self.streaming:
                self.paused = False

    def stop(self):
        '''
        stop streamimg
        '''
        with self.streaming_lock:
            if self.streaming:
                self.streaming = False
