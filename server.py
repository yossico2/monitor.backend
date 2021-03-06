import sys
import signal
import redis
import socketio
import eventlet
import threading
import random
from threading import Timer
from datetime import datetime, timedelta, timezone
from dateutil import parser
from typing import Dict

import config
from streamer import Streamer
from redis_cache import RedisCache
from state_sql import StateSQL
from state_updater import StateUpdater

import logging
logger = logging.getLogger(__name__)
#lilo: logging.basicConfig(level=logging.DEBUG)


def signal_handler(sig, frame):
    print('user exit.')
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


class MonitorServer:

    def __init__(self):
        self._clients: Dict[str, Streamer] = dict()
        self._clients_lock = threading.Lock()

        self.sio = socketio.Server(cors_allowed_origins='*')

        self.redis_cache = RedisCache(
            redis_client=redis.Redis(config.REDIS_HOST),
            redis_ttl_sec=config.REDIS_TTL_SEC)

        self.state_SQL = StateSQL(
            sql_host=config.SQL_HOST,
            sql_user=config.SQL_USER,
            sql_password=config.SQL_PASSWORD
        )
        self.state_SQL.init_db()

        self.state_updater = StateUpdater(
            sio=self.sio,
            redis_cache=self.redis_cache,
            state_SQL=self.state_SQL)

        self.sio.on('connect', self.on_connect)
        self.sio.on('disconnect', self.on_disconnect)
        # datagen-events: should actually come from kafka topic
        self.sio.on('datagen-events', self.state_updater.on_datagen_events)
        self.sio.on('pb-fetch-range', self.on_fetch_range)
        self.sio.on('pb-stream', self.on_stream)
        self.sio.on('pb-pause', self.on_pause)
        self.sio.on('pb-play', self.on_play)
        self.sio.on('pb-stop', self.on_stop)
        self.sio.on('pb-fetch-power-data', self.on_fetch_power_block_data)

    def start(self):
        print('starting monitor backend server ... ')
        self.state_updater.start()
        app = socketio.WSGIApp(self.sio)
        eventlet.wsgi.server(eventlet.listen(('', config.SERVER_PORT)), app)

    def stop(self):
        if self.state_updater:
            self.state_updater.stop()

    def on_connect(self, sid: str, environ):
        print(f'>>> client connected (sid: {sid})')
        with self._clients_lock:
            self._clients[sid] = Streamer(
                sid=sid,
                sio=self.sio,
                redis_cache=self.redis_cache,
                state_SQL=self.state_SQL)

    def on_disconnect(self, sid: str):
        print(f'>>> client disconnected {sid}')
        with self._clients_lock:
            client_streamer = self._clients.get(sid)
            if client_streamer:
                client_streamer.stop()
            self._clients[sid] = None

    def on_fetch_range(self, sid: str, start_date: str, end_date: str):
        '''
        fetch events between (start_date, end_date)
        '''
        # lilo:TODO
        print(f'>>> fetch_range (sid:{sid}) start_date: {start_date} end_date: {end_date}')
        start_date = parser.parse(start_date)
        end_date = parser.parse(end_date)
        client_streamer = self._clients.get(sid)
        client_streamer.fetch(start_date, end_date)

    def on_stream(self, sid: str, start_date_str: str):
        '''
        start streamimg events from start_date
        '''
        client_streamer = self._clients.get(sid)
        start_date = parser.parse(start_date_str)
        client_streamer.stream(start_date)

    def on_pause(self, sid: str):
        '''
        pause streamimg
        '''
        print(f'>>> on_pause (sid:{sid})')
        client_streamer = self._clients.get(sid)
        client_streamer.pause()

    def on_play(self, sid: str):
        '''
        play
        '''
        print(f'>>> on_play (sid:{sid})')
        client_streamer = self._clients.get(sid)
        client_streamer.play()

    def on_stop(self, sid: str):
        '''
        stop
        '''
        print(f'>>> on_stop (sid:{sid})')
        client_streamer = self._clients.get(sid)
        client_streamer.stop()

    def on_fetch_power_block_data(self, sid: str, timestamp: datetime):
        '''
        called by client to fetch power data within power-block
        '''
        # lilo: for now just simulate events
        print(
            f'>>> on_fetch_power_block_data (sid: {sid}, timestamp: {timestamp})')
        data = []
        frequency = 2 * 1e9
        numPoints = random.randint(0, 1000)
        maxStepSize = 500000 / numPoints
        for i in range(numPoints):
            frequency += (random.random() + 0.01) * maxStepSize
            power = random.randint(0, 100)
            data.append({
                'index': i,
                'frequency': frequency,
                'power': power
            })

        self.sio.emit('pb-data', data=data, to=sid)


def run_test_mode(server):
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

    # play/pause timer
    if True:
        def pause(paused):
            if paused:
                server.on_pause(sid=sid)
            else:
                server.on_play(sid=sid)

            t = Timer(interval=3 if paused else 5,
                      function=pause, args=(not paused,))
            t.start()  # pause after X seconds

        pause(paused=False)

    # TEST stream
    if True:
        print('stream TEST:')
        server.on_stream(
            sid=sid,
            start_date=datetime.now(tz=timezone.utc))

    input("press any key\n")


if __name__ == "__main__":

    # simulate data
    from datagen import DataGenerator
    data_generator = DataGenerator(es_host=config.ES_HOST)
    data_generator.start()

    # start monitor server
    server = MonitorServer()
    if config.TEST_MODE:
        run_test_mode(server)
    else:
        server.start()
