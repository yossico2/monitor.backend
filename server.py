import socketio
import eventlet
from datetime import datetime

sio = socketio.Server()
app = socketio.WSGIApp(sio)

@sio.event
def connect(sid, environ):
    print('connect ', sid)

def generate_event():
    while True:
        message = f'lilo {datetime.now()}'
        print(f'emit: {message}')
        sio.emit('event', message)
        sio.sleep(1)

@sio.event
def disconnect(sid):
    print('disconnect ', sid)

if __name__ == '__main__':
    sio.start_background_task(generate_event)
    eventlet.wsgi.server(eventlet.listen(('', 5000)), app)
