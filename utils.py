from datetime import datetime, timezone


def datetime_to_ms_since_epoch(dt: datetime) -> int:
    return round(dt.timestamp()*1000)


def ms_since_epoch_to_datetime(ms_since_epoch) -> datetime:
    return datetime.fromtimestamp(ms_since_epoch/1000, tz=timezone.utc)

# if __name__ == "__main__":
#     utc_now = datetime.now(tz=timezone.utc)
#     ms_since_epoch = datetime_to_ms_since_epoch(utc_now)
#     print(ms_since_epoch)
#     utc_now2 = ms_since_epoch_to_datetime(ms_since_epoch)
#     print(datetime_to_ms_since_epoch(utc_now2))
