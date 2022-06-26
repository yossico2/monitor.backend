import sys
import signal
from datetime import datetime
import config

def signal_handler(sig, frame):
    print('user exit.')
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":

    # simulate data
    from datagen import DataGenerator
    data_generator = DataGenerator(es_host=config.ES_HOST)
    data_generator.start(start_time=datetime.now())

    # time.sleep(3) # run for 3 sec
    # data_generator.stop()
    # print('done.')
    print('generating data to es ... ')
    input("press ctrl-c to exit\n")
