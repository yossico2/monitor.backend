import abc
from json import tool
import time
import json
from operator import index
import typing
from datetime import datetime, timedelta, timezone
from typing import Generic, Type, TypeVar, List

import redis

from elasticsearch.client import Elasticsearch
from elasticsearch_dsl import Search

import pydantic
from pydantic import BaseModel, validator
from pydantic.json import pydantic_encoder

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
    # timestamp: datetime
    timestamp: int


# bucket size in time units
BUCKET_TIMEDELTA = timedelta(seconds=1)


class Bucket(BaseModel):
    '''
    A bucket is the unit of fetching/caching data.
    '''

    start: datetime

    @property
    def end(self) -> datetime:
        return self.start + BUCKET_TIMEDELTA

    @validator("start")
    def align_start(cls, v: datetime) -> datetime:
        '''Align bucket start date.'''
        seconds_in_bucket = BUCKET_TIMEDELTA.total_seconds()
        return datetime.fromtimestamp((v.timestamp() // seconds_in_bucket * seconds_in_bucket), timezone.utc)

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

    Fetch and return the list of records between start_date (inclusive) and end_date (exclusive).

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

    def fetch(self, start_date: datetime, end_date: datetime) -> List[T]:

        # initialize a list of buckets in the date range
        buckets = self._init_buckets(start_date, end_date)

        records: list[T] = []
        for bucket in buckets:

            # Check if there's anything in cache
            # lilo: mget(redis)?
            cache_key = self.bucket_cache_key(bucket)
            cached_raw_value = self.get_redis_client().get(cache_key)
            if cached_raw_value is not None:
                # cache hit
                print(f'<<< cache hit')
                records += pydantic.parse_raw_as(
                    list[self.get_model_type()],
                    cached_raw_value  # type: ignore
                )
                continue

            # cache miss
            # Fetch the value from the upstream
            # lilo: mget(es)?
            print('<<< fetch data from upstream')
            values = self.get_values_from_upstream(bucket)

            whole_bucket_fetched = len(
                values) > 0 and values[-1].timestamp == utils.to_epoch_millisec(bucket.end) - 100

            # lilo: data may not be available yet in upstream (e.g: start_date/end_date in the future)
            # (trim empty results)

            # Save the value to the cache
            if whole_bucket_fetched:
                # lilo: mset(redis)?
                raw_values = json.dumps(
                    values,
                    separators=(",", ":"),
                    default=pydantic_encoder)

                self.get_redis_client().set(
                    cache_key,
                    raw_values,
                    ex=self.get_cache_ttl(bucket))

            records += values

        start_date_ms = utils.to_epoch_millisec(start_date)
        end_date_ms = utils.to_epoch_millisec(end_date)
        hits = [record for record in records if start_date_ms <=
                record.timestamp < end_date_ms]
        return hits

    def _init_buckets(self, start_date: datetime, end_date: datetime) -> List[Bucket]:
        '''
        return a list of buckets in a date range.
        '''
        buckets: list[Bucket] = []

        if end_date < start_date:
            raise ValueError(f"end_date must be greater than start_date")

        bucket = Bucket(start=start_date)

        while True:
            buckets.append(bucket)
            bucket = bucket.next()
            if bucket.end >= end_date:
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

    Fetch and return the list of PowerBlock records between start_date (inclusive) and end_date (exclusive).

    Usage example:
        fetcher = PowerBlockFetcher(redis_client, redis_ttl_sec, es, es_index)
        power_blocks = fetcher.fetch(start_date, end_date)
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
        start_epoch_millisec = utils.to_epoch_millisec(bucket.start)
        return f'power_blocks:{start_epoch_millisec}'

    def get_cache_ttl(self, bucket: Bucket):
        # all buckets have the same ttl
        return self.redis_ttl_sec

    def get_values_from_upstream(self, bucket: Bucket) -> List[PowerBlock]:
        return self.fetch_power_blocks(bucket.start, bucket.end)

    # ----------------------------------------------------------------------------------
    # Query
    # ----------------------------------------------------------------------------------

    def fetch_power_blocks(
            self,
            start_date: datetime,
            end_date: datetime) -> List[PowerBlock]:
        '''
        Low-level query to fetch power_blocks from ElasticSearch.
        Notice how it doesn't know anything about caching or date buckets. 
        Its job is to pull some data from ElasticSearch, convert them to model 
        instances, and return them as a list.
        '''

        logger.info(
            f"fetch power_blocks from the upstream for ({start_date}, {end_date})")

        search = Search(using=self.es, index=self.power_blocks_index).filter(
            "range",
            timestamp={
                "gte": utils.to_epoch_millisec(start_date),
                "lt": utils.to_epoch_millisec(end_date),
            },
        ).sort('timestamp').params(preserve_order=True)

        # We use scan() instead of execute() to fetch all the records, and wrap it with a
        # list() to pull everything in memory. As long as we fetch data in buckets of
        # limited sizes, memory is not a problem.
        result = list(search.scan())
        blocks = [
            PowerBlock(
                # lilox
                # timestamp=datetime.fromtimestamp(record.timestamp).replace(tzinfo=timezone.utc),

                timestamp=record.timestamp,
                frequency=record["frequency"],
                power=record["power"],
            )
            for record in result
        ]

        return blocks

# ----------------------------------------------------------------------------------
# Streamer
# ----------------------------------------------------------------------------------


class Streamer:
    def __init__(self, sid: str, sio: socketio.Server) -> None:

        # SocketIO
        self.sid = sid  # sio client socket id
        self.sio = sio

        self.streaming = False

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

    def fetch(self, start_date: datetime, end_date: datetime):
        '''
        fetch events between (start_date, end_date)
        '''

        power_blocks = self.fetcher.fetch(start_date, end_date)

        json_power_blocks = [
            json.dumps(power_block.dict())
            for power_block in power_blocks
        ]

        self.sio.emit('power_blocks', data=json_power_blocks, to=self.sid)

    def _wait_for_es_data(self, start_date: datetime):
        '''
        Given start_date wait until a whole bucket is available for fetching from upstream.

        (this introduces an inherented latency of bucket-size)
        '''

        bucket = Bucket(start=start_date)

        while True:
            if not self.streaming:
                return  # stop streaming

            # get item with latest timestamp
            s = Search(using=self.es, index=config.ES_POWER_BLOCKS_INDEX).filter(
                "range", timestamp={"gte": utils.to_epoch_millisec(bucket.end)},
            ).sort('timestamp')

            s = s[0:1]  # we only need one

            result = list(s.execute())
            if len(result) > 0:
                break  # done: we have es data available for start_date

            print(f'no data available yet at: {start_date}')
            time.sleep(1)  # wait 1 sec for data to arrive

    def stream(self, start_date: datetime):
        '''
        start streamimg events from start_date
        '''

        # fail if already streaming
        if self.streaming:
            raise RuntimeError('already streaming!')
        self.streaming = True

        # wait for es data with timestamp >= start_date
        self._wait_for_es_data(start_date)

        # start streaming
        while self.streaming:
            if not self.streaming:
                return  # stop streaming

            # timing
            if config.DEBUG_STREAMER:
                timing_start = time.time()

            # fetch 1 sec
            end_date = start_date + timedelta(seconds=1)
            power_blocks = self.fetcher.fetch(start_date, end_date)
            # print(f'lilo ----------- len(power_blocks): {len(power_blocks)}')

            # timing
            if config.DEBUG_STREAMER:
                duration_ms = round(1000*(time.time() - timing_start))
                print(
                    f'fetched {len(power_blocks)} power_blocks (duration: {duration_ms} ms)')

            # lilo: TODO - to_json + sio.emit

            time.sleep(1)
            if len(power_blocks) > 0:
                # +100 ms
                new_start_timestamp = power_blocks[-1].timestamp + 100
                start_date = datetime.fromtimestamp(
                    new_start_timestamp / 1e3,
                    tz=timezone.utc)

    def pause(self):
        '''
        pause streamimg
        '''
        # lilo:TODO
        if not self.streaming:
            return

    def stop(self):
        '''
        stop streamimg
        '''
        # lilo:TODO
        if not self.streaming:
            return
