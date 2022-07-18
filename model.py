from typing import TypeVar
from pydantic import BaseModel
from enum import Enum


class TimestampModel(BaseModel):
    '''
    Base model for any records having timestamps.
    '''
    timestamp: int


class PowerBlock(TimestampModel):
    '''
    Model to keep track of an individual PowerBlock.
    '''

    frequency: float
    power: int
    state: int = 0

 # lilo: T = TypeVar("T", bound=TimestampModel)
T = TypeVar("T", bound=PowerBlock)

class States(Enum):
    init = 0
    request = 1
    response = 2
    resolved = 3
