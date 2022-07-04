import time
import math
import random
import threading
import socketio
from datetime import datetime, timezone
from elasticsearch_dsl import Document, Date, Integer, Float
from elasticsearch_dsl.connections import connections
from elasticsearch.helpers import bulk

import config
import utils


class PowerBlock_Document(Document):

    timestamp = Date(required=True, format="epoch_millis")
    frequency = Float(required=True)
    power = Integer(required=True)

    class Index:
        name = config.ES_POWER_BLOCKS_INDEX


class DataGenerator:
    def __init__(self, es_host: str):

        # define a default es-client
        self.connection = connections.create_connection(hosts=[es_host])

        # sio thread-events
        self.event_sio_thread_start = threading.Event()
        self.event_sio_thread_stop = threading.Event()

        # gendata thread-events
        self.event_gendata_thread_start = threading.Event()
        self.event_gendata_thread_stop = threading.Event()
        self.stop_flag = False

        # create the mappings in elasticsearch
        PowerBlock_Document.init(
            using=self.connection, index=config.ES_POWER_BLOCKS_INDEX)

    def start(self):

        print('starting datagen server ... ')

        # start and wait for gendata thread
        self.thread = threading.Thread(target=self.gendata_thread, daemon=True)
        self.thread.start()
        self.event_gendata_thread_start.wait()

    def stop(self):
        if not self.stop_flag:
            self.stop_flag = True
            self.event_gendata_thread_stop.wait()
            self.connection.close()
            if config.DEBUG_DATAGEN:
                print(f'data generation stopped.')

            # lilo:TODO - stop sio thread

    def gendata_thread(self):
        '''
        generate PowerBlock_Document objects
        bulk save to es every 1 sec.
        '''

        # signal gendata thread started
        self.event_gendata_thread_start.set()

        # sio.client connects to datagen
        self.sio = socketio.Client()
        server_sio_address = f'http://localhost:{config.SERVER_PORT}'
        while True:
            try:
                print(f'datagen connecting to: {server_sio_address}')
                self.sio.connect(server_sio_address, wait=False, wait_timeout=0.1)
                break
            except:
                time.sleep(0.1) # sec

        start_date = datetime.now(tz=timezone.utc)

        if config.DEBUG_DATAGEN:
            ts = utils.datetime_to_ms_since_epoch(start_date)
            print(f'datagen (start_date: {ts})')

        # start_date (align to 100 ms start)
        aligned_start_date = datetime.fromtimestamp(math.floor(
            10*start_date.timestamp())/10, tz=start_date.tzinfo)
        timestamp = utils.datetime_to_ms_since_epoch(aligned_start_date)

        # generate data
        while not self.stop_flag:

            # timing
            if config.DEBUG_DATAGEN:
                timing_start = time.time()

            # collect PowerBlock_Document objects for bulk index
            docs: list(PowerBlock_Document) = []

            for _ in range(10):  # bulk 10 objects every 1 sec.

                doc = PowerBlock_Document(
                    meta={'id': timestamp},
                    timestamp=timestamp,
                    frequency=random.randrange(1e3, 40e9),
                    power=random.randrange(10, 100))

                docs.append(doc)

                # advance time
                timestamp += config.PERIOD_MS

            # bulk index
            bulk(self.connection, (b.to_dict(include_meta=True) for b in docs))

            # emit events to sio
            docs_json = [d.to_dict() for d in docs]
            self.sio.emit('datagen-events', data=docs_json)

            # timing
            if config.DEBUG_DATAGEN:
                duration_ms = round(1000*(time.time() - timing_start))
                print(f'es-bulk {len(docs)} items ({duration_ms} ms)')

            # sleep (100ms)
            time.sleep(config.PERIOD_MS/1000)

        self.event_gendata_thread_stop.set()  # signal thread stopped


# if __name__ == "__main__":
#     # simulate data
#     print('generating data to es ... ')
#     data_generator = DataGenerator(es_host=config.ES_HOST)
#     data_generator.start()
#     time.sleep(1)
#     data_generator.stop()
