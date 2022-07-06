from typing import TypeVar
from pydantic import BaseModel


class TimestampModel(BaseModel):
    '''
    Base model for any records having timestamps.
    '''
    timestamp: int

