import json
import random
import socketio
import eventlet
from datetime import datetime, timezone
from pydantic.json import pydantic_encoder

import config
from model import PowerBlock
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

    def on_datagen_events(self, sid: str, events_json):
        events_dict = json.loads(events_json)
        power_blocks = [PowerBlock(**d) for d in events_dict]
        for pb in power_blocks:
            self.ring.push(pb)
        if config.DEBUG_STATE_UPDATE:
            print(f'on_datagen_events: ({self.ring.count()} events)')

    def update_events_state(self):
        while True:
            if self.stop_flag:
                break

            events = self.ring.toArray()
            if len(events) > 0:
                pb_updated = []
                for e in events:
                    if self.update_event_state(e):
                        pb_updated.append(e)
                if len(pb_updated) > 0:
                    # lilo:TODO
                    # update cache
                    self.redis_cache.update_items(pb_updated)

                    # update db
                    # lilo:TODO

                    # emit to clients
                    pb_updated_json = json.dumps(pb_updated, default=pydantic_encoder)
                    self.sio.emit('pb-state-updates', data=pb_updated_json)
                    print(f'lilo ----------- pb-state-updates: ({len(pb_updated)} items)')

            eventlet.sleep(1)

    def update_event_state(self, e):

        now = utils.datetime_to_ms_since_epoch(datetime.now(tz=timezone.utc))
        if now - e.timestamp < 2000:
            #  unmodified
            # lilox (now < e.timestamp)?
            return False

        state = e.state

        if state == stateInit:
            if now - e.timestamp > 5000:
                return False  # unmodified
            if random.random() < 0.5:
                return False  # unmodified
            e.state = stateRequest
            return True

        if state == stateRequest:
            if now - e.timestamp > 7000:
                return False  # unmodified
            if random.random() < 0.2:
                return False  # unmodified (failed to get response)
            e.state = stateResponse
            return True

        if state == stateResponse:
            if now - e.timestamp > 7000:
                return False  # unmodified
            if random.random() < 0.95:
                return False  # unmodified (not resolved)
            e.state = stateResolved
            return True

        if state == stateResolved:
            return False  # unmodified

        return False  # unmodified
