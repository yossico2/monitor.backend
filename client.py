import socketio

sio = socketio.Client()

@sio.event
def connect():
    print('connection established.')

@sio.on('event')
def on_event(data):
    print(f'event: {data}')

@sio.event
def disconnect():
    print('disconnected from server.')

sio.connect('http://localhost:5000')
sio.wait()
