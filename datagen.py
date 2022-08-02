import json
import time
import random
import threading
import socketio
import eventlet
from datetime import datetime, timezone
from elasticsearch_dsl import Document, Date, Integer, Float
from elasticsearch_dsl.connections import connections
from elasticsearch.helpers import bulk

import config
from state_updater import SEC
import utils


def _now_ms():
    return utils.datetime_to_ms_since_epoch(datetime.now(tz=timezone.utc))


class PowerBlock_Document(Document):
    '''
    Elasticsearch document model for PowerBlock
    '''

    # lilo: sensor_id = Integer(required=True)
    timestamp = Date(required=True, format="epoch_millis")
    frequency = Float(required=True)
    power = Integer(required=True)

    class Index:
        name = config.ES_POWER_BLOCKS_INDEX


class DataGenerator:
    def __init__(self, es_host: str):

        # define a default es-client
        self.connection = connections.create_connection(hosts=[es_host])

        # sio client
        self.sio = socketio.Client()

        # gendata thread-events
        self.event_gendata_thread_start = threading.Event()
        self.event_gendata_thread_stop = threading.Event()
        self.stop_flag = False

        # create the mappings in elasticsearch
        PowerBlock_Document.init(
            using=self.connection, index=config.ES_POWER_BLOCKS_INDEX)

    def start(self):

        print('starting datagen server ... ')
        self.thread = self.sio.start_background_task(self.gendata_background_task)
        self.event_gendata_thread_start.wait()

    def stop(self):
        if not self.stop_flag:
            self.stop_flag = True
            self.event_gendata_thread_stop.wait()
            self.connection.close()
            if config.DEBUG_DATAGEN:
                print(f'data generation stopped.')

    def sio_connect_to_server(self):
        server_sio_address = f'http://localhost:{config.SERVER_PORT}'
        while True:
            try:
                print(f'datagen connecting to: {server_sio_address}')
                self.sio.connect(server_sio_address,
                                 wait=False, wait_timeout=0.1)
                break
            except:
                eventlet.sleep(1)  # sec

    def gendata_background_task(self):
        '''
        generate PowerBlock_Document objects
        bulk save to es every 1 sec.
        '''

        # signal gendata thread started
        self.event_gendata_thread_start.set()

        # connect sio.client
        self.sio_connect_to_server()

        # collect objects for bulk index
        docs: list(PowerBlock_Document) = []

        # bulk every 1 sec
        last_bulk = _now_ms()

        # generate data (while not stop flag)
        while not self.stop_flag:

            now = _now_ms()

            # create a new event with aligned timestamp
            now_aligned_period = now // config.PERIOD_MS * config.PERIOD_MS
            doc = PowerBlock_Document(
                meta={'id': now_aligned_period},
                timestamp=now_aligned_period,
                frequency=random.randrange(1e3, 40e9),
                power=random.randrange(10, 100))

            # collect into bulk buffer
            docs.append(doc)

            # bulk every 1 sec
            if now - last_bulk > SEC:
                # timing
                if config.DEBUG_DATAGEN:
                    timing_start = time.time()

                # bulk to es
                bulk(self.connection, (b.to_dict(include_meta=True)
                     for b in docs))
                last_bulk = now

                # emit to sio
                docs_dict = [d.to_dict() for d in docs]
                docs_json = json.dumps(docs_dict)
                self.sio.emit('datagen-events', data=docs_json)

                # timing
                if config.DEBUG_DATAGEN:
                    duration_ms = round(1000*(time.time() - timing_start))
                    print(f'es-bulk {len(docs)} items ({duration_ms} ms)')

                # clear bulk buffer
                docs = []

            # sleep PERIOD_MS
            eventlet.sleep(config.PERIOD_MS/1000)

        self.event_gendata_thread_stop.set()  # signal thread stopped
