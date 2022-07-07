from threading import Timer
from datetime import datetime, timezone


class RepeatTimer(Timer):
    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)


def datetime_to_ms_since_epoch(dt: datetime) -> int:
    return round(dt.timestamp()*1000)


def ms_since_epoch_to_datetime(ms_since_epoch) -> datetime:
    return datetime.fromtimestamp(ms_since_epoch/1000, tz=timezone.utc)
