import time
import datetime
import json
import random
import socketio
import eventlet
import dataclasses
from ring import Ring
import threading

sio = socketio.Server(cors_allowed_origins='*')

# event cache
sec = 1000
minute = 60 * sec
cacheSize = 5 * minute
period = 100  # event every 100ms
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
    index: int = 0
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


_clients = {}
_clients_lock = threading.Lock()


@sio.event
def connect(sid, environ):
    print(f'client connected {sid}')
    with _clients_lock:
        _clients[sid] = None
    # with _ring_lock:
    #     events = _ring.toArray()
    #     json_events = []
    #     for e in events:
    #         json_events.append(json.dumps(e, cls=DataclassJSONEncoder))
    #     sio.emit('initialize-events', json_events)
    #     print(f'initialize-events: {len(events)}')


@sio.event
def disconnect(sid):
    print(f'client disconnected {sid}')


@dataclasses.dataclass
class Session:
    start_time: int = 0
    background_task: object = None
    active: bool = False


def stream_events_task(sid, start_time):
    print(
        f'lilo ------- stream-events: sid {sid} start_time: {start_time} date: {datetime.datetime.fromtimestamp(start_time/1000.0)}')

    last_time_sent = 0

    while True:

        events = []
        sio.sleep(0)
        with _ring_lock:
            events = _ring.toArray()

        for event in events:
            if not event:
                continue
            
            if event.time < start_time:
                continue # skip

            if event.time < last_time_sent:
                continue # skip

            # send over sio as json
            json_event = json.dumps(event, cls=DataclassJSONEncoder)
            sio.emit('event', data=json_event, to=sid)
            last_time_sent = event.time

            print(f'sid: {sid}, event: {event}')

            sio.sleep(period/1000)

            with _clients_lock:
                session = _clients.get(sid)
                if not session:
                    return
                if not session.active:
                    return


@sio.on('stream-events')
def stream_events(sid, start_time):

    with _clients_lock:
        session = _clients.get(sid)
        if session:
            # clear client session
            session.active = False
            session.background_task.join()

        session = Session(start_time=start_time, background_task=None)
        _clients[sid] = session
        session.background_task = sio.start_background_task(
            stream_events_task, sid=sid, start_time=start_time)
        session.active = True


def generate_events():

    event_timestamp = int(time.time() * 1000)  # start time in ms since epoch

    index = -1
    while True:

        index += 1

        event = Event(
            index=index,
            time=event_timestamp,
            lastUpdate=event_timestamp,
            state=stateArray[stateInitial],
            frequency=round(random.random() * 40000, 2)
        )

        # push to cache
        with _ring_lock:
            _ring.push(event)
        # print(f'event: {event}')

        # send over sio as json
        # json_event = json.dumps(event, cls=DataclassJSONEncoder)
        # sio.emit('event', json_event)

        sio.sleep(period/1000)
        event_timestamp += period


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
            print(f'event-update: {e} delay: {_ring.last().time - e.time}')
            # break


if __name__ == '__main__':
    sio.start_background_task(generate_events)
    sio.start_background_task(update_events)

    app = socketio.WSGIApp(sio)
    eventlet.wsgi.server(eventlet.listen(('', 5000)), app)
