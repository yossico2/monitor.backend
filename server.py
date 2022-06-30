import sys
import signal
import socketio
import eventlet
import threading
from datetime import datetime, timedelta, timezone
from typing import Dict
from streamer import Streamer

import config


def signal_handler(sig, frame):
    print('user exit.')
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


class MonitorServer:

    def __init__(self):
        self._clients: Dict[str, Streamer] = dict()
        self._clients_lock = threading.Lock()

        self.sio = socketio.Server(cors_allowed_origins='*')
        self.sio.on('connect', self.on_connect)
        self.sio.on('disconnect', self.on_disconnect)
        self.sio.on('fetch-events', self.on_fetch)
        self.sio.on('stream-events', self.on_stream)
        self.sio.on('pause-events', self.on_pause)

    def start(self):
        app = socketio.WSGIApp(self.sio)
        eventlet.wsgi.server(eventlet.listen(('', config.SERVER_PORT)), app)

    def on_connect(self, sid: str, environ):
        print(f'client connected (sid: {sid})')
        with self._clients_lock:
            self._clients[sid] = Streamer(sid=sid, sio=self.sio)

    def on_disconnect(self, sid: str):
        print(f'client disconnected {sid}')
        with self._clients_lock:
            client_streamer = self._clients.get(sid)
            if client_streamer:
                client_streamer.stop()
            self._clients[sid] = None

    def on_fetch(self, sid: str, start_date: datetime, end_date: datetime):
        '''
        fetch events between (start_date, end_date)
        '''
        client_streamer = self._clients.get(sid)
        client_streamer.fetch(start_date, end_date)

    def on_stream(self, sid: str, start_date: datetime):
        '''
        start streamimg events from start_date
        '''
        client_streamer = self._clients.get(sid)
        client_streamer.stream(start_date)

    def on_pause(self, sid: str):
        '''
        pause streamimg
        '''
        client_streamer = self._clients.get(sid)
        client_streamer.pause()


if __name__ == "__main__":

    # simulate data into es
    import time
    from datagen import DataGenerator
    data_generator = DataGenerator(es_host=config.ES_HOST)
    data_generator.start(start_date=datetime.now(tz=timezone.utc))
    # time.sleep(1)
    # data_generator.stop()

    # start monitor server
    server = MonitorServer()
    # print('starting monitor backend server ... ')
    # server.start()

    # TEST
    sid = 'lilo'
    server.on_connect(sid=sid, environ={})

    # TEST fetch
    if False:
        print('fetch TEST:')
        end_date = datetime.now(tz=timezone.utc)
        start_date = end_date - timedelta(seconds=1)
        server.on_fetch(
            sid=sid,
            start_date=start_date,
            end_date=end_date)

    # TEST stream
    if True:
        print('stream TEST:')
        server.on_stream(
            sid=sid,
            start_date=datetime.now(tz=timezone.utc))


    input("press any key\n")