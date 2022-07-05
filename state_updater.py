from distutils.command.config import config
from ring import Ring
import config

# constants
# -------------------
SEC = 1000  # ms
MINUTE = 60 * SEC
PERIOD = 100  # ms

stateInit = 0
stateRequest = 1
stateResponse = 2
stateResolved = 3

class StateUpdater:
    def __init__(self):

        ring_size = 5 * MINUTE / PERIOD
        self.ring = Ring(ring_size)

    
    def start_update_timer(self):
        pass

    def on_datagen_events(self, sid:str, events):
        # lilo:TODO
        for e in events:
            e['state'] = stateInit
            self.ring.push(e)
        print(f'on_datagen_events: ({self.ring.count()} events)')
