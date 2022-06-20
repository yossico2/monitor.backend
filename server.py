import socketio
import eventlet
from time import time
import random
from datetime import datetime
from ring import Ring
from dataclasses import dataclass
import dataclasses
import json

sio = socketio.Server(cors_allowed_origins='*')

app = socketio.WSGIApp(sio)


@sio.event
def connect(sid, environ):
    print('client connected ', sid)


@sio.event
def disconnect(sid):
    print('client disconnected ', sid)


@dataclass
class Event:
    time: int
    state: int
    frequency: int


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


sec = 1000
minute = 60 * sec
cacheSize = 5 * minute
period = 100
ringSize = cacheSize / period

ring = Ring(ringSize)

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


def generate_events():

    while True:
        event = Event(
            time=round(time(), 1),  # round to 100ms
            state=stateArray[stateNormal],
            frequency=round(random.random() * 40000, 2)
        )

        ring.push(event)
        json_event = json.dumps(event, cls=EnhancedJSONEncoder)
        sio.emit('event', json_event)

        sio.sleep(0.1)


def update_event(e):
    now = time()
    timestamp = e.time
    if now - timestamp < 2:
        return False  # unmodified

    # update state
    if stateNormal == e.state:
        if now - timestamp > 5000:
            return False  # unmodified
        if random.random() < 0.5:
            return False  # unmodified
        e.state = stateRequest
        return True

    if stateRequest == e.state:
        if now - timestamp > 7000:
            return False  # unmodified
        if random.random() < 0.2:
            return False  # unmodified (failed to get response)
        e.state = stateResponse
        return True

    if stateResponse == e.state:
        if now - timestamp > 7000:
            return False  # unmodified
        if random.random() < 0.95:
            return False  # unmodified (not resolved)
        e.state = stateResolved
        return True

    if stateResolved == e.state:
        return False # unmodified

    return False  # unmodified


def update_events():

    sio.sleep(1)

    while True:

        for e in ring.toArray():
            if None == e:
                continue
            if not update_event(e):
                continue  # unmodified
            json_event = json.dumps(e, cls=EnhancedJSONEncoder)
            sio.emit('event-update', json_event)

        sio.sleep(1)


if __name__ == '__main__':
    sio.start_background_task(generate_events)
    sio.start_background_task(update_events)
    eventlet.wsgi.server(eventlet.listen(('', 5000)), app)
