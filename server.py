import sys
import signal
from datetime import datetime
from typing import Dict
import socketio
import eventlet
import threading
import config
from streamer import Streamer


def signal_handler(sig, frame):
    print('user exit.')
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


class MonitorServer:

    def __init__(self):
        self._clients: Dict[str, Streamer] = dict()
        self._clients_lock = threading.Lock()

    def start(self):
        sio = socketio.Server(cors_allowed_origins='*')
        sio.on('connect', self.on_connect)
        sio.on('disconnect', self.on_disconnect)
        app = socketio.WSGIApp(sio)
        eventlet.wsgi.server(eventlet.listen(('', config.SERVER_PORT)), app)

    def on_connect(self, sid, environ):
        print(f'client connected {sid}')

        with self._clients_lock:
            self._clients[sid] = Streamer()

    def on_disconnect(self, sid):
        print(f'client disconnected {sid}')
        with self._clients_lock:
            client_streamer = self._clients.get(sid)
            if client_streamer:
                client_streamer.stop()
            self._clients[sid] = None


if __name__ == "__main__":

    # simulate data
    # from datagen import DataGenerator
    # print('generating data to es ... ')
    # data_generator = DataGenerator(es_host=config.ES_HOST)
    # data_generator.start(start_time=datetime.now())
    # input("press ctrl-c to exit\n")
    # data_generator.stop()

    print('starting monitor backend server ... ')
    server = MonitorServer()
    server.start()
