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
stateNormal = 0
stateRequest = 1
stateResponse = 2
stateResolved = 3

stateArray = [
    stateNormal,
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
            state=stateArray[stateNormal],
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

    # lilo
    now = int(time.time() * 1000)  # start time in ms since epoch
    if now - e.lastUpdate < 2000:
        return False  # unmodified

    # update state
    if stateNormal == e.state:
        if now - e.time > 5000:
            return False  # unmodified
        if random.random() < 0.5:
            return False  # unmodified
        e.state = stateRequest
        e.lastUpdate = now
        return True

    if stateRequest == e.state:
        if now - e.time > 7000:
            return False  # unmodified
        if random.random() < 0.2:
            return False  # unmodified (failed to get response)
        e.state = stateResponse
        e.lastUpdate = now
        return True

    if stateResponse == e.state:
        if now - e.time > 15000:
            return False  # unmodified
        if random.random() < 0.80:
            return False  # unmodified (not resolved)
        e.state = stateResolved
        e.lastUpdate = now
        return True

    if stateResolved == e.state:
        return False  # unmodified

    return False  # unmodified


def update_events():

    while True:

        with _ring_lock:
            events = _ring.toArray()

        for e in events:

            if not update_event(e):
                continue  # unmodified

            json_event = json.dumps(e, cls=DataclassJSONEncoder)
            sio.emit('event-update', json_event)
            print(f'event-update: {e}')
            break

        sio.sleep(0.1)


if __name__ == '__main__':
    sio.start_background_task(generate_events)
    sio.start_background_task(update_events)

    app = socketio.WSGIApp(sio)
    eventlet.wsgi.server(eventlet.listen(('', 5000)), app)
