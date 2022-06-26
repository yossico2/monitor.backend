import abc
import json
import typing
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Generic, Type, TypeVar, List

import redis

from elasticsearch.client import Elasticsearch
from elasticsearch_dsl import Q, Search

import pydantic
from pydantic import BaseModel, validator
from pydantic.json import pydantic_encoder

import socketio

import config

import logging
logger = logging.getLogger(__name__)
#lilo: logging.basicConfig(level=logging.DEBUG)

BUCKET_TIMEDELTA = timedelta(seconds=60)


class TimestampModel(BaseModel):
    '''
    Base model for any records having timestamps.
    '''
    timestamp: datetime


class Bucket(BaseModel):
    '''
    Model to represent a bucket, the unit of fetching data.
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

    The fetcher uses cached records if available. Fetch records from the upstream if not in cache and update the cache.

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
    def get_cache_key(self, bucket: Bucket) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_cache_ttl(self, bucket: Bucket):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_values_from_upstream(self, bucket: Bucket) -> List[T]:
        raise NotImplementedError()

    @abc.abstractmethod
    def redis_client(self):
        raise NotImplementedError()

    def fetch(self, start_date: datetime, end_date: datetime) -> List[T]:

        # initialize a list of (empty) buckets in a date range
        buckets = self._init_buckets(start_date, end_date)

        model_type = self._get_model_type()

        records: list[T] = []
        for bucket in buckets:

            # Check if there's anything in cache
            cache_key = self.get_cache_key(bucket)
            cached_raw_value = self.redis_client().get(cache_key)
            if cached_raw_value is not None:
                records += pydantic.parse_raw_as(
                    list[model_type], cached_raw_value  # type: ignore
                )
                continue

            # Fetch the value from the upstream
            value = self.get_values_from_upstream(bucket)

            # Save the value to the cache
            raw_value = json.dumps(
                value,
                separators=(",", ":"),
                default=pydantic_encoder)

            self.redis_client().set(
                cache_key,
                raw_value,
                ex=self.get_cache_ttl(bucket))

            records += value

        return [record for record in records if start_date <= record.timestamp < end_date]

    def _get_model_type(self) -> Type:
        '''
        Python non-documented black magic to fetch current model.
        Ref: https://stackoverflow.com/a/60984681
        '''
        parametrized_generic_fetcher = self.__orig_bases__[0]  # type: ignore
        return typing.get_args(parametrized_generic_fetcher)[0]

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
        fetcher = PowerBlockFetcher(redis_client, redis_ttl_sec, es_client, es_index)
        power_blocks = fetcher.fetch(start_date, end_date)
    '''

    def __init__(self, redis_client, redis_ttl_sec, es_client, es_index):
        self.redis_client = redis_client
        self.es_client = es_client
        self.index = es_index
        self.redis_ttl_sec = redis_ttl_sec

    def redis_client(self):
        return self.redis_client

    def get_cache_key(self, bucket: Bucket) -> str:
        '''return the cache key by the bucket start date.'''
        # round to 100ms boundary
        block_time = round(self.start.timestamp(), 1)
        return f'power_blocks:{block_time}'

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

        search = (
            self.es_client.index(self.index)
            .filter(
                "range",
                timestamp={
                    "gte": int(start_date.timestamp()),
                    "lt": int(end_date.timestamp()),
                },
            )
        )

        # We use scan() instead of execute() to fetch all the records, and wrap it with a
        # list() to pull everything in memory. As long as we fetch data in buckets of
        # limited sizes, memory is not a problem.
        result = list(search.scan())
        return [
            PowerBlock(
                timestamp=datetime.fromtimestamp(
                    record.timestamp).replace(tzinfo=timezone.utc),
                frequency=record["frequency"],
                power=record["power"],
            )
            for record in result
        ]

# ----------------------------------------------------------------------------------
# Streamer
# ----------------------------------------------------------------------------------


class Streamer:
    def __init__(self, sio: socketio.Server) -> None:

        # Redis (cache)
        redis_client = redis.Redis(config.REDIS_HOST)

        # ElasticSearch (upstream)
        es_client = Search(using=Elasticsearch(hosts=[config.ES_HOST]))

        self.fetcher = PowerBlockFetcher(
            redis_client=redis_client,
            es_client=es_client,
            es_index=config.ES_INDEX,
            redis_ttl_sec=config.REDIS_TTL_SEC
        )
        # power_blocks = fetcher.fetch(start_date, end_date)

    def stream(self, start_date: datetime):
        pass

    def stop(self):
        pass
