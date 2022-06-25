import abc
import json
import logging
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

# ----------------------------------------------------------------------------------
# Global constants
# ----------------------------------------------------------------------------------
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
ES_URL = 'http://localhost:9200'
REDIS_URL = '127.0.0.1'

# ----------------------------------------------------------------------------------
# ElasticSearch (upstream)
# ----------------------------------------------------------------------------------
es_client = Search(using=Elasticsearch(hosts=[ES_URL]))

# ----------------------------------------------------------------------------------
# Redis (cache)
# ----------------------------------------------------------------------------------
redis_client = redis.Redis(REDIS_URL)


class TimestampModel(BaseModel):
    """Base model for any records having timestamps."""
    timestamp: datetime


bucket_timedelta = timedelta(seconds=60)


class Bucket(BaseModel):
    """Model to represent a bucket, the unit of fetching data."""

    start: datetime

    @property
    def end(self) -> datetime:
        return self.start + bucket_timedelta

    @validator("start")
    def align_start(cls, v: datetime) -> datetime:
        """Align bucket start date."""
        seconds_in_bucket = bucket_timedelta.total_seconds()
        return datetime.fromtimestamp((v.timestamp() // seconds_in_bucket * seconds_in_bucket), timezone.utc)

    def next(self) -> "Bucket":
        """Return the next bucket."""
        return Bucket(start=self.end)

    def cache_key(self) -> str:
        """Helper function to return the cache key by the bucket start date."""
        return f"{int(self.start.timestamp())}"

    class Config:
        frozen = True


def get_buckets(start_date: datetime, end_date: datetime) -> List[Bucket]:
    """Return the list of (empty) buckets in a date range."""
    buckets: list[Bucket] = []

    if end_date < start_date:
        raise ValueError(f"{end_date=} must be greater than {start_date=}")

    bucket = Bucket(start=start_date)
    while True:
        buckets.append(bucket)
        bucket = bucket.next()
        if bucket.end >= end_date:
            break
    return buckets

# ----------------------------------------------------------------------------------
# Cache utils
# ----------------------------------------------------------------------------------


T = TypeVar("T", bound=TimestampModel)


class GenericFetcher(abc.ABC, Generic[T]):
    """Generic cache fetcher.
    Notice the following things.
    - We define the GenericFetcher as an abstract base class (inherited from ABC)
      and defined some methods as abstract. It's an extra saftey net. The interpreter
      prohibits instantiation of abstract classes or their subclasses without all
      abstract methods defined. Read more about abstract base
      classes at https://docs.python.org/3/library/abc.html
    - On top of this, we define this class as "generic." In our case, it means that
      subclasses can specify with types methods  fetch() and get_values_from_upstream()
      will return. Read more: https://mypy.readthedocs.io/en/stable/generics.html
    """

    @abc.abstractmethod
    def get_cache_key(self, bucket: Bucket) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_values_from_upstream(self, bucket: Bucket) -> List[T]:
        raise NotImplementedError()

    def fetch(self, start_date: datetime, end_date: datetime) -> List[T]:
        """The main class entrypoint.
        Fetch and return the list of records between start_date (inclusive) and
        end_date (exclusive).
        """
        buckets = get_buckets(start_date, end_date)
        model_type = self.get_model_type()
        records: list[T] = []
        for bucket in buckets:

            # Check if there's anything in cache
            cache_key = self.get_cache_key(bucket)
            cached_raw_value = redis_client.get(cache_key)
            if cached_raw_value is not None:
                records += pydantic.parse_raw_as(
                    list[model_type], cached_raw_value  # type: ignore
                )
                continue

            # Fetch the value from the upstream
            value = self.get_values_from_upstream(bucket)

            # Save the value to the cache
            raw_value = json.dumps(
                value, separators=(",", ":"), default=pydantic_encoder
            )
            redis_client.set(cache_key, raw_value, ex=get_cache_ttl(bucket))

            records += value

        return [
            record for record in records if start_date <= record.timestamp < end_date
        ]

    def get_model_type(self) -> Type:
        """Python non-documented black magic to fetch current model.
        Ref: https://stackoverflow.com/a/60984681
        """
        parametrized_generic_fetcher = self.__orig_bases__[0]  # type: ignore
        return typing.get_args(parametrized_generic_fetcher)[0]


def get_cache_ttl(bucket: Bucket):
    if bucket.end >= datetime.now(tz=timezone.utc):
        return int(timedelta(minutes=10).total_seconds())
    return int(timedelta(days=30).total_seconds())

# ----------------------------------------------------------------------------------
# PowerBlock Fetcher
# ----------------------------------------------------------------------------------


class PowerBlock(TimestampModel):
    """Model to keep track of the individual PowerBlock."""

    amount: Decimal
    power_block_hash: str

    class Config:
        # Unless strongly necessary otherwise, we prefer define our models as frozen.
        frozen = True


class PowerBlockFetcher(GenericFetcher[PowerBlock]):
    """PowerBlock fetcher.
    Usage example:
        fetcher = PowerBlockFetcher("1HesYJSP1QqcyPEjnQ9vzBL1wujruNGe7R")
        power_blocks = fetcher.fetch(start_date, end_date)
    """

    def __init__(self, power_blocks_address: str):
        self.power_blocks_address = power_blocks_address

    def get_cache_key(self, bucket: Bucket) -> str:
        return f"power_blocks:{self.power_blocks_address}:{bucket.cache_key()}"

    def get_values_from_upstream(self, bucket: Bucket) -> List[PowerBlock]:
        return fetch_power_blocks(self.power_blocks_address, bucket.start, bucket.end)


# ----------------------------------------------------------------------------------
# Query
# ----------------------------------------------------------------------------------


def fetch_power_blocks(
    power_blocks_address: str, start_date: datetime, end_date: datetime
) -> List[PowerBlock]:
    """Low-level query to fetch power_blocks from ElasticSearch.
    Notice how it doesn't know anything about caching or date buckets. Its job is to
    pull some data from ElasticSearch, convert them to model instances, and return
    them as a list.
    """
    logger.info(
        f"Fetch power_blocks for {power_blocks_address=} "
        f"({start_date=}, {end_date=}) from the upstream"
    )

    search = (
        es_client.index("output")
        .filter(
            "range",
            timestamp={
                "gte": int(start_date.timestamp()),
                "lt": int(end_date.timestamp()),
            },
        )
        .filter(
            "nested",
            path="scriptPubKey",
            query=Q(
                "bool", filter=[Q("terms", scriptPubKey__addresses=[power_blocks_address])]
            ),
        )
    )

    # We use scan() instead of execute() to fetch all the records, and wrap it with a
    # list() to pull everything in memory. As long as we fetch data in buckets of
    # limited sizes, memory is not a problem.
    result = list(search.scan())
    return [
        PowerBlock(
            timestamp=datetime.fromtimestamp(record.timestamp).replace(
                tzinfo=timezone.utc
            ),
            amount=record["value"]["raw"],
            power_block_hash=record["power_blockHash"],
        )
        for record in result
    ]


if __name__ == "__main__":
    pass
