from sqlite3 import Timestamp
import time
import math
import random
import threading
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

    # def save(self, ** kwargs):
    #     self.power = len(self.body.split())
    #     return super(PowerBlock_Document, self).save(** kwargs)


class DataGenerator:
    def __init__(self, es_host):

        # define a default elasticsearch client
        self.connection = connections.create_connection(hosts=[es_host])

        # manage thread
        self.event_thread_start = threading.Event()
        self.event_thread_stop = threading.Event()
        self.stop_flag = False

        # create the mappings in elasticsearch
        PowerBlock_Document.init(
            using=self.connection, index=config.ES_POWER_BLOCKS_INDEX)

    def start(self, start_date: datetime):

        self.thread = threading.Thread(
            target=self.generate_data, args=(start_date,), daemon=True)
        self.thread.start()
        self.event_thread_start.wait()

    def stop(self):
        if not self.stop_flag:
            self.stop_flag = True
            self.event_thread_stop.wait()
            self.connection.close()
            if config.DEBUG_DATAGEN:
                print(f'data generation stopped.')

    def generate_data(self, start_date: datetime):
        '''
        generate PowerBlock_Document objects
        bulk save to es every 1 sec.
        '''

        self.event_thread_start.set()  # signal started

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

                docs.append(
                    PowerBlock_Document(
                        meta={'id': timestamp},
                        timestamp=timestamp,
                        frequency=random.randrange(1e3, 40e9),
                        power=random.randrange(10, 100)))

                # advance time
                timestamp += config.PERIOD_MS

            # bulk index
            # bulk(self.connection, (b.to_dict(include_meta=True)
            #      for b in docs), refresh='true')
            bulk(self.connection, (b.to_dict(include_meta=True) for b in docs))

            # timing
            if config.DEBUG_DATAGEN:
                duration_ms = round(1000*(time.time() - timing_start))
                print(f'es-bulk {len(docs)} items ({duration_ms} ms)')

            # sleep 100ms
            time.sleep(config.PERIOD_MS/1000)

        self.event_thread_stop.set()  # signal stopped


if __name__ == "__main__":
    # simulate data
    print('generating data to es ... ')
    data_generator = DataGenerator(es_host=config.ES_HOST)
    data_generator.start(start_date=datetime.now(tz=timezone.utc))
    time.sleep(1)
    data_generator.stop()
