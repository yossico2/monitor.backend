import time
import redis

r = redis.Redis()

keys = [f'power:field{i+1}' for i in range(5)]

for key in keys:

    # create timeseries
    if not r.exists(key):
        r.ts().create(key=key, duplicate_policy='last',
                      labels={'class': 'power'})

start = time.time()

pipe = r.ts().pipeline(transaction=True)

# add values for 60 sec
num_values = 60*10

# now = round(time.time() * 1000)
now = 0

# # add
# for key in keys:
#     timestamp = now
#     for i in range(num_values):
#         pipe.add(key=key, timestamp=timestamp, value=i)
#         timestamp += 100  # 100ms

# madd
timestamp = now
for i in range(num_values):
    pipe.madd([(f, timestamp, i) for f in keys])
    timestamp += 100  # 100ms

pipe.execute(raise_on_error=True)

end = time.time()
print(f'{num_values} data points, duration: {end - start}')


start = time.time()
res = r.ts().mrange(0, 1000, filters=["class=power"], bucket_size_msec=1000)
end = time.time()
print(f'get data points, duration: {end - start}')
print(res)
