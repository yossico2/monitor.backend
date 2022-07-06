import pydantic
import redis
import typing
from datetime import datetime
from pydantic import BaseModel, validator
from typing import Generic, Type, TypeVar, List

import utils
from model import TimestampModel

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


T = TypeVar("T", bound=TimestampModel)

class RedisCache(Generic[T]):
    def __init__(self, redis_client: redis.Redis):
        self.redis_client = redis_client
        self.model_type = None

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

    def bucket_cache_key(self, bucket_start: int) -> str:
        '''return the cache key by the bucket start date.'''
        return f'power_blocks:{bucket_start}'

    def align_bucket_start(self, timestamp: int) -> int:
        '''Align timestamp to bucket start.'''
        return timestamp // BUCKET_TIMEDELTA * BUCKET_TIMEDELTA

    def get_item(self, timestamp:int) -> T:
        # check if cache contains bucket
        bucket_start = self.align_bucket_start(timestamp)
        bucket_cache_key = self.bucket_cache_key(bucket_start)
        cached_raw_value = self.redis_client.get(bucket_cache_key)
        if cached_raw_value is not None:
            # cache hit
            cached_items: list[T] = pydantic.parse_raw_as(
                list[self.get_model_type()],
                cached_raw_value  # type: ignore
            )
        
        for item in cached_items:
            if item.timestamp == timestamp:
                return item
        return None
    
    def get_items(self, start_timestamp: int, end_timestamp: int) -> List[T]:
        if end_timestamp < start_timestamp:
            raise ValueError(f"end-timestamp must be greater than start-timestamp")

        # initialize a list of buckets in range
        buckets = self._init_buckets(start_timestamp, end_timestamp)

        data_items: list[T] = []
        for bucket in buckets:

            # check if cache contains bucket
            cache_key = self.bucket_cache_key(bucket.start)
            cached_raw_value = self.get_redis_client().get(cache_key)
            if cached_raw_value is not None:

                # cache hit
                cached_items: list[T] = pydantic.parse_raw_as(
                    list[self.get_model_type()],
                    cached_raw_value  # type: ignore
                )

                data_items += cached_items
                continue

        # return only items in range
        items_in_range = [
            item for item in data_items if start_timestamp <= item.timestamp < end_timestamp
        ]

        return items_in_range