import abc
import time
import json
import typing
import threading
import eventlet
from datetime import datetime
from typing import Generic, Type, TypeVar, List

from elasticsearch.client import Elasticsearch
from elasticsearch_dsl import Search

import pydantic
from pydantic import BaseModel, validator
from pydantic.json import pydantic_encoder

import redis
import socketio

import config
import utils

import logging
logger = logging.getLogger(__name__)
#lilo: logging.basicConfig(level=logging.DEBUG)


class TimestampModel(BaseModel):
    '''
    Base model for any records having timestamps.
    '''
    timestamp: int


# bucket size in time units
BUCKET_TIMEDELTA = 1000  # ms


class Bucket(BaseModel):
    '''
    A bucket is the unit of fetching/caching data.
    '''

    start: int

    @property
    def end(self) -> int:
        return self.start + BUCKET_TIMEDELTA

    @validator("start")
    def align_start(cls, start: int) -> int:
        '''Align bucket start date.'''
        return start // BUCKET_TIMEDELTA * BUCKET_TIMEDELTA

    def next(self) -> "Bucket":
        '''Return the next bucket.'''
        return Bucket(start=self.end)

    class Config:
        frozen = True

# ----------------------------------------------------------------------------------
# Cache utils
# ----------------------------------------------------------------------------------


T = TypeVar("T", bound=TimestampModel)


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
    def bucket_cache_key(self, bucket: Bucket) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_cache_ttl(self, bucket: Bucket):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_values_from_upstream(self, bucket: Bucket) -> List[T]:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_redis_client(self):
        raise NotImplementedError()

    model_type = None

    def get_model_type(self) -> Type:
        '''
        Python non-documented black magic to fetch current model.
        Ref: https://stackoverflow.com/a/60984681
        '''
        if not self.model_type:
            parametrized_generic_fetcher = self.__orig_bases__[
                0]  # type: ignore
            self.model_type = typing.get_args(parametrized_generic_fetcher)[0]
        return self.model_type

    def fetch(self, start_date_ms: int, end_date_ms: int) -> List[T]:

        if end_date_ms < start_date_ms:
            raise ValueError(f"end-date must be greater than start-date")

        # Initialize a list of buckets in range
        buckets = self._init_buckets(start_date_ms, end_date_ms)

        data_items: list[T] = []
        for bucket in buckets:

            # Check if cache contains bucket
            cache_key = self.bucket_cache_key(bucket)
            cached_raw_value = self.get_redis_client().get(cache_key)
            if cached_raw_value is not None:

                # Cache Hit
                cached_items: list[T] = pydantic.parse_raw_as(
                    list[self.get_model_type()],
                    cached_raw_value  # type: ignore
                )

                data_items += cached_items
                continue

            # Cache Miss (fetch bucket from the upstream)
            fetched_items = self.get_values_from_upstream(bucket)

            # save the value to the cache only when whole block fetched
            # (partial blocks are not cached, thus will be fetched again when required)
            whole_bucket_fetched = len(
                fetched_items) > 0 and fetched_items[-1].timestamp == (bucket.end - config.PERIOD_MS)

            # Cache items
            if whole_bucket_fetched:
                raw_values = json.dumps(
                    fetched_items,
                    separators=(",", ":"),
                    default=pydantic_encoder)

                self.get_redis_client().set(
                    cache_key,
                    raw_values,
                    ex=self.get_cache_ttl(bucket))

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


class PowerBlock(TimestampModel):
    '''
    Model to keep track of an individual PowerBlock.
    '''

    frequency: float
    power: int

    class Config:
        frozen = True


class PowerBlockFetcher(GenericFetcher[PowerBlock]):
    '''
    PowerBlock fetcher.

    Fetch and return the list of PowerBlock records between start-date (inclusive) and end-date (exclusive).

    Usage example:
        fetcher = PowerBlockFetcher(redis_client, redis_ttl_sec, es, es_index)
        power_blocks = fetcher.fetch(start-date, end-date)
    '''

    def __init__(self, redis_client: redis.Redis, redis_ttl_sec: int, es: Elasticsearch, power_blocks_index: str):
        self.redis_client = redis_client
        self.es = es
        self.power_blocks_index = power_blocks_index
        self.redis_ttl_sec = redis_ttl_sec

    def get_redis_client(self):
        return self.redis_client

    def bucket_cache_key(self, bucket: Bucket) -> str:
        '''return the cache key by the bucket start date.'''
        return f'power_blocks:{bucket.start}'

    def get_cache_ttl(self, bucket: Bucket):
        # all buckets have the same ttl
        return self.redis_ttl_sec

    def get_values_from_upstream(self, bucket: Bucket) -> List[PowerBlock]:
        '''
        Pull from ElasticSearch data items in bucket range, convert them to model 
        instances, and return them as a list.
        '''

        start_date_ms = bucket.start
        end_date_ms = bucket.end

        # timing
        if config.DEBUG_STREAMER:
            fetch_timing_start = time.time()

        # ElasticSearch query
        s = Search(using=self.es, index=self.power_blocks_index).filter(
            'range',
            timestamp={
                'gte': start_date_ms,
                'lt': end_date_ms,
            },
        )  # .sort('timestamp')

        # We use scan() instead of execute() to fetch all the records, and wrap it with a
        # list() to pull everything in memory. As long as we fetch data in buckets of
        # limited sizes, memory is not a problem.
        records = list(s.params(preserve_order=True).scan())

        blocks = [
            PowerBlock(
                timestamp=record.timestamp,
                frequency=record.frequency,
                power=record.power,
            )
            for record in records
        ]

        # timing
        if config.DEBUG_STREAMER:
            duration_ms = round(1000*(time.time() - fetch_timing_start))
            print(f'{len(blocks)} items fetched from upstream ({duration_ms} ms)')

        return blocks

# ----------------------------------------------------------------------------------
# Streamer
# ----------------------------------------------------------------------------------


class Streamer:
    def __init__(
        self,
        sid: str,
        sio: socketio.Server
    ) -> None:

        # SocketIO
        self.sid = sid  # sio client socket id
        self.sio = sio

        self.streaming = False
        self.paused = False

        self.streaming_lock = threading.Lock()

        # Redis (cache)
        redis_client = redis.Redis(config.REDIS_HOST)

        # ElasticSearch (upstream)
        self.es = Elasticsearch(hosts=[config.ES_HOST])

        self.fetcher = PowerBlockFetcher(
            redis_client=redis_client,
            es=self.es,
            power_blocks_index=config.ES_POWER_BLOCKS_INDEX,
            redis_ttl_sec=config.REDIS_TTL_SEC
        )

    def _sleep(self, duration):
        eventlet.sleep(duration)

    def fetch(self, start_date: datetime, end_date: datetime):
        '''
        fetch events between (start_date, end_date)
        '''
        start_date_ms = utils.datetime_to_ms_since_epoch(start_date)
        end_date_ms = utils.datetime_to_ms_since_epoch(end_date)

        power_blocks = self.fetcher.fetch(start_date_ms, end_date_ms)

        power_blocks_json = [
            json.dumps(power_block, default=pydantic_encoder)
            for power_block in power_blocks
        ]

        self.sio.emit('power-blocks', data=power_blocks_json, to=self.sid)

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
                print(f'--->>> paused')
                self._sleep(config.PERIOD_MS/1000)
                continue  # pause

            #  fetch items
            power_blocks: PowerBlock = self.fetcher.fetch(
                start_date_ms, end_date_ms)
            if len(power_blocks) == 0:
                # either no data in range or data not available yet in upstream
                # (e.g: start-date/end-date in the future)
                result = self._upstream_data_after_date(end_date_ms)
                if len(result) == 0:
                    # no data >= end_date_ms available in upstream
                    self._sleep(config.PERIOD_MS/1000)
                    continue

                # advance range (skip data gap)
                # continue from where we have data
                start_date_ms = result[0].timestamp
                end_date_ms = start_date_ms + BUCKET_TIMEDELTA
                continue

            # stream power_blocks
            for power_block in power_blocks:

                with self.streaming_lock:
                    if not self.streaming:
                        return  # stop streaming

                while self.paused:
                    self._sleep(config.PERIOD_MS/1000)
                    continue  # pause

                power_block_json = json.dumps(
                    power_block, default=pydantic_encoder)

                self.sio.emit('power-blocks',
                              data=power_block_json, to=self.sid)

                #  print timestamp of power_block
                if config.DEBUG_STREAMER:
                    print(f'emit {power_block.timestamp}')

                self._sleep(config.PERIOD_MS/1000)

            # advance range (skip no data)
            start_date_ms = power_blocks[-1].timestamp + config.PERIOD_MS
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
