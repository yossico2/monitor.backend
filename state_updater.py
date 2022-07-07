import random
import socketio
from datetime import datetime, timezone

import config
import utils
from ring import Ring
from redis_cache import RedisCache

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
    def __init__(self, sio: socketio.Server, redis_cache: RedisCache):
        self.sio = sio
        self.redis_cache = redis_cache
        ring_size = 5 * MINUTE / PERIOD
        self.ring = Ring(ring_size, key='timestamp')
        self.thread = None
        self.stop_flag = False

    def start(self):
        self.thread = self.sio.start_background_task(self.update_events_state)

    def stop(self):
        if self.thread:
            self.stop_flag = True
            self.thread.join()

    def on_datagen_events(self, sid: str, events):
        for e in events:
            e['state'] = stateInit
            self.ring.push(e)
        if config.DEBUG_STATE_UPDATE:
            print(f'on_datagen_events: ({self.ring.count()} events)')

    def update_events_state(self):
        while True:
            if self.stop_flag:
                break

            events = self.ring.toArray()
            if len(events) > 0:
                updated = []
                for e in events:
                    if self.update_event_state(e):
                        updated.append(e)
                if len(updated) > 0:
                    # lilo:TODO
                    # update cache
                    self.redis_cache.update_items(updated)

                    # update db
                    # lilo:TODO

                    # emit to clients
                    self.sio.emit('pb-events-state', data=updated)

            self.sio.sleep(SEC/PERIOD)

    def update_event_state(self, e):

        now = utils.datetime_to_ms_since_epoch(datetime.now(tz=timezone.utc))
        timestamp = e['timestamp']
        if now - timestamp < 2000:
            #  unmodified
            return False

        state = e['state']

        if state == stateInit:
            if now - timestamp > 5000:
                return False  # unmodified
            if random.random() < 0.5:
                return False  # unmodified
            e['state'] = stateRequest
            return True

        if state == stateRequest:
            if now - timestamp > 7000:
                return False  # unmodified
            if random.random() < 0.2:
                return False  # unmodified (failed to get response)
            e['state'] = stateResponse
            return True

        if state == stateResponse:
            if now - timestamp > 7000:
                return False  # unmodified
            if random.random() < 0.95:
                return False  # unmodified (not resolved)
            e['state'] = stateResolved
            return True

        if state == stateResolved:
            return False  # unmodified

        return False  # unmodified
