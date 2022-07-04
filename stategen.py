from distutils.command.config import config
from ring import Ring
import config

# constants
# -------------------
SEC = 1000  # ms
MINUTE = 60 * SEC
PERIOD = 100  # ms


class StateUpdater:
    def __init__(self):

        ring_size = 5 * MINUTE / PERIOD
        self.ring = Ring(ring_size)

    def on_datagen_events(self, sid:str, events):
        # lilo:TODO
        print(f'on_datagen_events: ({len(events)} events)')
