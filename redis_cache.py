import json
import redis
from typing_extensions import get_args
import pydantic
from pydantic import BaseModel, validator
from pydantic.json import pydantic_encoder
from typing import Generic, Type, List

from model import T

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


class RedisCache(Generic[T]):
    def __init__(self, redis_client: redis.Redis, redis_ttl_sec: int):
        self.redis_client = redis_client
        self.redis_ttl_sec = redis_ttl_sec  # all buckets have the same ttl
        self.model_type = None

    def get_item(self, timestamp: int) -> T:
        # check if cache contains bucket
        bucket_start = self._align_bucket_start(timestamp)
        bucket_cache_key = self._bucket_cache_key(bucket_start)
        cached_raw_value = self.redis_client.get(bucket_cache_key)
        if cached_raw_value is not None:
            # cache hit
            cached_items: list[T] = pydantic.parse_raw_as(
                List[self._get_model_type()],
                cached_raw_value  # type: ignore
            )

        for item in cached_items:
            if item.timestamp == timestamp:
                return item
        return None

    def get_items_in_range(self, start_timestamp: int, end_timestamp: int) -> List[T]:
        if end_timestamp < start_timestamp:
            raise ValueError(
                f"end-timestamp must be greater than start-timestamp")

        # initialize a list of buckets in range
        buckets = self._init_buckets(start_timestamp, end_timestamp)

        data_items: list[T] = []
        for bucket in buckets:

            # check if cache contains bucket
            cached_items = self.get_bucket(bucket)
            if len(cached_items) > 0:
                # cache hit
                data_items += cached_items
                continue

        # return only items in range
        items_in_range = [
            item for item in data_items if start_timestamp <= item.timestamp < end_timestamp
        ]

        return items_in_range

    def get_bucket(self, bucket: Bucket) -> List[T]:

        # return list of items from cache
        cached_items: list[T] = []

        bucket_cache_key = self._bucket_cache_key(bucket.start)
        cached_raw_value = self.redis_client.get(bucket_cache_key)
        if cached_raw_value is not None:
            # cache hit
            cached_items: list[T] = pydantic.parse_raw_as(
                List[self._get_model_type()],
                cached_raw_value  # type: ignore
            )

        return cached_items

    def update_items(self, items: List[T]):

        # sort items by timestamp
        items = sorted(items, key=lambda d: d.timestamp)

        # collect buckets containing items to update
        cached_items = self.get_items_in_range(
            start_timestamp=items[0].timestamp,
            end_timestamp=items[-1].timestamp + BUCKET_TIMEDELTA)

        # create a dictionary of updated items by timestamp
        items_by_timestamp = {
            item.timestamp: item for item in items
        }

        # walk over all cached items, replacing the ones updated
        new_items = []
        for cached_item in cached_items:
            updated_item = items_by_timestamp.get(cached_item.timestamp)
            if updated_item:
                new_items.append(updated_item)
            else:
                new_items.append(cached_item)

        # put items back in cache
        self.set_items(new_items)

    def set_items(self, items: List[T]):

        items_length = len(items)
        if items_length == 0:
            return

        # sort items by timestamp
        items = sorted(items, key=lambda x: x.timestamp)

        buckets = self._init_buckets(items[0].timestamp, items[-1].timestamp)

        item_index = 0
        bucket_items = []

        for bucket in buckets:

            # collect bucket items
            while item_index < items_length:
                item = items[item_index]
                item_index += 1
                if item.timestamp >= bucket.end:
                    break
                bucket_items.append(item)

            # cache bucket items
            json_items = json.dumps(bucket_items, default=pydantic_encoder)

            bucket_cache_key = self._bucket_cache_key(bucket.start)
            self.redis_client.set(
                bucket_cache_key,
                json_items,
                ex=self.redis_ttl_sec)

            # prepare for next bucket
            bucket_items = []

    def _init_buckets(self, start_timestamp: int, end_timestamp: int) -> List[Bucket]:
        '''
        return a list of buckets in a date range.
        '''
        buckets: list[Bucket] = []

        bucket = Bucket(start=start_timestamp)

        while True:
            buckets.append(bucket)
            bucket = bucket.next()
            if bucket.end >= end_timestamp:
                break

        return buckets

    def _bucket_cache_key(self, bucket_start: int) -> str:
        '''return the cache key by the bucket start date.'''
        return f'power_blocks:{bucket_start}'

    def _align_bucket_start(self, timestamp: int) -> int:
        '''Align timestamp to bucket start.'''
        return timestamp // BUCKET_TIMEDELTA * BUCKET_TIMEDELTA

    def _get_model_type(self) -> Type:
        '''
        Python non-documented black magic to fetch current model.
        Ref: https://stackoverflow.com/a/60984681
        '''
        if not self.model_type:
            parametrized_generic_fetcher = self.__orig_bases__[
                0]  # type: ignore
            self.model_type = get_args(parametrized_generic_fetcher)[0]
        return self.model_type
