from datetime import datetime


def to_epoch_millis(dt: datetime):
    return int(dt.timestamp() * 1000)
