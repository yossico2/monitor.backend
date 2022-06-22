import time
import json
import random
import socketio
import eventlet
import dataclasses
from ring import Ring
import threading

sio = socketio.Server(cors_allowed_origins='*')


@sio.event
def connect(sid, environ):
    print(f'client connected {sid}')


@sio.event
def disconnect(sid):
    print(f'client disconnected {sid}')


# event cache
sec = 1000
minute = 60 * sec
cacheSize = 5 * minute
period = 100
ringSize = cacheSize / period
_ring = Ring(ringSize)

_ring_lock = threading.Lock()

# event states
stateInitial = 0
stateRequest = 1
stateResponse = 2
stateResolved = 3

stateArray = [
    stateInitial,
    stateRequest,
    stateResponse,
    stateResolved
]


@dataclasses.dataclass
class Event:
    time: int = 0
    lastUpdate: int = 0
    state: int = 0
    frequency: int = 0


class DataclassJSONEncoder(json.JSONEncoder):
    '''dataclass json encoder'''

    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


def generate_events():

    period_ms = 100

    event_timestamp = int(time.time() * 1000)  # start time in ms since epoch

    while True:
        event = Event(
            time=event_timestamp,
            lastUpdate=event_timestamp,
            state=stateArray[stateInitial],
            frequency=round(random.random() * 40000, 2)
        )

        # push to ring
        with _ring_lock:
            _ring.push(event)
        print(f'event: {event}')

        # send over sio as json
        json_event = json.dumps(event, cls=DataclassJSONEncoder)
        sio.emit('event', json_event)

        sio.sleep(period_ms/1000)
        event_timestamp += period_ms


def update_event(e):

    if None == e:
        return False

    now = int(time.time() * 1000)  # start time in ms since epoch

    # update state
    # initial -> request
    if stateInitial == e.state:
        if now - e.time < 2000:
            return False  # do not change state before
        e.state = stateRequest
        e.lastUpdate = now
        return True

    # request -> response
    if stateRequest == e.state:
        if now - e.lastUpdate < 10000:
            return False  # do not change state before lastUpdate
        if now - e.lastUpdate > 10000:
            return False  # do not change state after lastUpdate
        if random.random() < 0.10:
            return False  # x% don't get response
        e.state = stateResponse
        e.lastUpdate = now
        return True

    # response -> resolved
    if stateResponse == e.state:
        if now - e.lastUpdate < 10000:
            return False  # do not change state before lastUpdate
        if now - e.lastUpdate > 10000:
            return False  # do not change state after lastUpdate
        if random.random() < 0.99:
            return False  # x% don't resolve
        e.state = stateResolved
        e.lastUpdate = now
        return True

    # stateResolved don't change their state
    return False  # unmodified


def update_events():

    while True:

        sio.sleep(0)
        with _ring_lock:
            events = _ring.toArray()

        for e in events:
            if not update_event(e):
                continue  # unmodified
            json_event = json.dumps(e, cls=DataclassJSONEncoder)
            sio.emit('event-update', json_event)
            print(f'event-update: {e}')
            # break


if __name__ == '__main__':
    sio.start_background_task(generate_events)
    sio.start_background_task(update_events)

    app = socketio.WSGIApp(sio)
    eventlet.wsgi.server(eventlet.listen(('', 5000)), app)
