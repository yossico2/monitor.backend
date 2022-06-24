from cProfile import label
from sqlite3 import Date
import time
import redis

r = redis.Redis()


keys = [f'sensor:field{i}' for i in range(5)]

# add values for 60 sec
num_values = 60*10

for key in keys:

    # create timeseries
    if not r.exists(key):
        r.ts().create(key=key, duplicate_policy='last')


start = time.time()

pipe = r.ts().pipeline(transaction=True)

# now = round(time.time() * 1000)
now = 0

for key in keys:
    timestamp = now
    for i in range(num_values):
        pipe.add(key=key, timestamp=timestamp, value=i)
        timestamp += 100  # 100ms

pipe.execute(raise_on_error=True)

end = time.time()
print(f'{num_values} data points, duration: {end - start}')


pipe = r.ts().pipeline(transaction=True)
pipe.