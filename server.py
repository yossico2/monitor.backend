import socketio
import eventlet
from time import time
import random
from datetime import datetime
import json

sio = socketio.Server()
app = socketio.WSGIApp(sio)


@sio.event
def connect(sid, environ):
    print('connect ', sid)


def generate_events():
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

    while True:
        event = {
            'time': round(time(), 1),  # round to 100ms
            'data': {
                'state': stateArray[stateNormal],
                'frequency': round(random.random() * 40000, 2)
            }
        }

        sio.emit('event', json.dumps(event))

        sio.sleep(0.1)


@sio.event
def disconnect(sid):
    print('disconnect ', sid)


if __name__ == '__main__':
    sio.start_background_task(generate_events)
    eventlet.wsgi.server(eventlet.listen(('', 5000)), app)
