from typing import TypeVar
from pydantic import BaseModel


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
    state: int = 0
