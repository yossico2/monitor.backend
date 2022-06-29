import time
import math
import random
import threading
from elasticsearch_dsl import Document, Date, Integer, Float
from elasticsearch_dsl.connections import connections
from datetime import datetime, timezone
from elasticsearch.helpers import bulk
import config
import utils

PERIOD_MS = 100  # 100 ms


class PowerBlock_Document(Document):

    timestamp = Date(required=True, format="epoch_millis")
    frequency = Float(required=True)
    power = Integer(required=True)

    class Index:
        name = config.ES_POWER_BLOCKS_INDEX
        settings = {
            "number_of_shards": 2,
        }

    def save(self, ** kwargs):
        self.power = len(self.body.split())
        return super(PowerBlock_Document, self).save(** kwargs)


def align_block_start(start_date: datetime) -> datetime:
    return datetime.fromtimestamp(math.floor(10*start_date.timestamp())/10, tz=start_date.tzinfo)


class DataGenerator:
    def __init__(self, es_host):

        # define a default elasticsearch client
        self.connection = connections.create_connection(hosts=[es_host])

        # manage thread
        self.event_thread_start = threading.Event()
        self.event_thread_stop = threading.Event()
        self.stop_flag = False

        # create the mappings in elasticsearch
        PowerBlock_Document.init()

    def start(self, start_date: datetime):

        self.thread = threading.Thread(
            target=self.generate_data, args=(start_date,), daemon=True)
        self.thread.start()
        self.event_thread_start.wait()

    def stop(self):
        if not self.stop_flag:
            self.stop_flag = True
            self.event_thread_stop.wait()
            if config.DEBUG_DATAGEN:
                print(f'data generation stopped.')

    def generate_data(self, start_date: datetime):
        '''
        generate PowerBlock_Document objects
        bulk save to es every 1 sec.
        '''

        self.event_thread_start.set()  # signal started

        if config.DEBUG_DATAGEN:
            print(
                f'generating data to es (start_date: {utils.datetime_to_ms_since_epoch(start_date)})')

        # start_date and timedelta
        timestamp = utils.datetime_to_ms_since_epoch(
            align_block_start(start_date))
        time_delta = PERIOD_MS

        # generate data
        while not self.stop_flag:

            # bulk timing
            if config.DEBUG_DATAGEN:
                timing_start = time.time()

            # collect PowerBlock_Document objects for bulk index
            power_blocks = []

            for i in range(10):  # bulk 10 objects every 1 sec.
                power_blocks.append(
                    PowerBlock_Document(
                        timestamp=timestamp,
                        frequency=random.randrange(1e3, 40e9),
                        power=random.randrange(10, 100)))

                # advance time
                timestamp += time_delta

            # bulk index
            bulk(self.connection, (b.to_dict(True)
                 for b in power_blocks), refresh='true')

            # bulk timing
            if config.DEBUG_DATAGEN:
                duration_ms = round(1000*(time.time() - timing_start))
                print(f'es-bulk {len(power_blocks)} items ({duration_ms} ms)')
            
            # sleep 100ms
            time.sleep(0.1)

        self.event_thread_stop.set()  # signal stopped


if __name__ == "__main__":
    # simulate data
    print('generating data to es ... ')
    data_generator = DataGenerator(es_host=config.ES_HOST)
    data_generator.start(start_date=datetime.now(tz=timezone.utc))
    time.sleep(1)
    data_generator.stop()
