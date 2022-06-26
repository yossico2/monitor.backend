import time
import random
import threading
from elasticsearch_dsl import Document, Date, Integer, Float
from elasticsearch_dsl.connections import connections
from datetime import timedelta
from elasticsearch.helpers import bulk
import config

DEBUG=False

class PowerBlock(Document):

    timestamp = Date()
    frequency = Float()
    power = Integer()

    class Index:
        name = config.ES_INDEX
        settings = {
          "number_of_shards": 2,
        }

    def save(self, ** kwargs):
        self.power = len(self.body.split())
        return super(PowerBlock, self).save(** kwargs)

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

    def start(self, start_time):
        
        self.thread = threading.Thread(target=self.generate_data, args=(start_time,), daemon=True)
        self.thread.start()
        self.event_thread_start.wait()

    def stop(self):
        if not self.stop_flag:
            self.stop_flag = True
            self.event_thread_stop.wait()

    def generate_data(self, start_time):
        '''
        generate PowerBlock objects
        bulk save to es every 1 sec.
        '''

        self.event_thread_start.set() # signal started

        # starttime and timedelta
        timestamp = start_time
        time_delta = timedelta(milliseconds=100)

        # generate data
        while not self.stop_flag:
            
            # bulk timing
            if DEBUG:
                timing_start = time.time()

            # aggregate PowerBlock objects for bulk index
            power_blocks = []

            for i in range(10 ): # bulk 10 objects every 1 sec.
                power_blocks.append(
                    PowerBlock(
                        timestamp=timestamp, 
                        frequency=random.randrange(1e3, 40e9), 
                        power=random.randrange(10, 100)))

                time.sleep(0.1) # sleep 100ms
                timestamp += time_delta

            # bulk index
            bulk(self.connection, (b.to_dict(True) for b in power_blocks))

            # bulk timing
            if DEBUG:
                duration = time.time() - timing_start
                print(f'duration: {duration} ms')

        self.event_thread_stop.set() # signal stopped
    