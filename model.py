import json
from typing import List, TypeVar
from pydantic import BaseModel
from pydantic.json import pydantic_encoder


class TimestampModel(BaseModel):
    '''
    Base model for any records having timestamps.
    '''
    timestamp: int


T = TypeVar("T", bound=TimestampModel)


class PowerBlock(TimestampModel):
    '''
    Model to keep track of an individual PowerBlock.
    '''

    frequency: float
    power: int

    class Config:
        frozen = True


def pydantic_to_json(items):
    if isinstance(items, list):
        json_items = json.dumps(
            items,
            separators=(",", ":"),
            default=pydantic_encoder)
        return json_items

    json_item = json.dumps(items, default=pydantic_encoder)
    return json_item
