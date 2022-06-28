from datetime import datetime


def to_epoch_millisec(dt: datetime):
    return int(dt.timestamp() * 1000)
