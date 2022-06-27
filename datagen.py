import time
import math
import random
import threading
from elasticsearch_dsl import Document, Date, Integer, Float
from elasticsearch_dsl.connections import connections
from datetime import datetime, timezone, timedelta
from elasticsearch.helpers import bulk
import config
import utils

DEBUG = False


class PowerBlock(Document):

    timestamp = Date(required=True, format="epoch_millis")
    frequency = Float(required=True)
    power = Integer(required=True)

    class Index:
        name = config.ES_INDEX
        settings = {
            "number_of_shards": 2,
        }

    def save(self, ** kwargs):
        self.power = len(self.body.split())
        return super(PowerBlock, self).save(** kwargs)


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
        PowerBlock.init()

    def start(self, start_date: datetime):

        self.thread = threading.Thread(
            target=self.generate_data, args=(start_date,), daemon=True)
        self.thread.start()
        self.event_thread_start.wait()

    def stop(self):
        if not self.stop_flag:
            self.stop_flag = True
            self.event_thread_stop.wait()

    def generate_data(self, start_date: datetime):
        '''
        generate PowerBlock objects
        bulk save to es every 1 sec.
        '''

        self.event_thread_start.set()  # signal started

        # start_date and timedelta
        # timestamp as total milliseconds since epoch
        epoch_millis = utils.to_epoch_millis(align_block_start(start_date))
        time_delta = 100

        # generate data
        while not self.stop_flag:

            # bulk timing
            if DEBUG:
                timing_start = time.time()

            # aggregate PowerBlock objects for bulk index
            power_blocks = []

            for i in range(10):  # bulk 10 objects every 1 sec.
                power_blocks.append(
                    PowerBlock(
                        timestamp=epoch_millis,
                        frequency=random.randrange(1e3, 40e9),
                        power=random.randrange(10, 100)))

                time.sleep(0.1)  # sleep 100ms
                epoch_millis += time_delta

            # bulk index
            bulk(self.connection, (b.to_dict(True) for b in power_blocks))

            # bulk timing
            if DEBUG:
                duration = time.time() - timing_start
                print(f'duration: {duration} ms')

        self.event_thread_stop.set()  # signal stopped


if __name__ == "__main__":
    # simulate data
    print('generating data to es ... ')
    data_generator = DataGenerator(es_host=config.ES_HOST)
    data_generator.start(start_date=datetime.now(tz=timezone.utc))
    time.sleep(3)  # sec
    data_generator.stop()
