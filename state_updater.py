import json
import random
import socketio
import eventlet
from datetime import datetime, timezone
from pydantic.json import pydantic_encoder
from typing import List

import config
from model import PowerBlock
import utils
from ring import Ring
from redis_cache import RedisCache
from state_sql import StateSQL

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
    def __init__(self, sio: socketio.Server, redis_cache: RedisCache, state_SQL: StateSQL):
        self.sio = sio
        self.redis_cache = redis_cache
        self.state_SQL = state_SQL
        ring_size = 5 * MINUTE / PERIOD
        self.ring = Ring(ring_size, key='timestamp')
        self.thread = None
        self.stop_flag = False

    def start(self):
        self.thread = self.sio.start_background_task(
            self.update_state_background_task)

    def stop(self):
        if self.thread:
            self.stop_flag = True
            self.thread.join()

    def on_datagen_events(self, sid: str, events_json):
        '''
        push events to Ring
        '''
        events_dict = json.loads(events_json)
        power_blocks = [PowerBlock(**d) for d in events_dict]
        for pb in power_blocks:
            self.ring.push(pb)
        if config.DEBUG_STATE_UPDATE:
            print(
                f'on_datagen_events: ({len(power_blocks)} events), ring size: {self.ring.count()}')

    def update_state_background_task(self):
        '''
        update events state in Ring
        '''

        while True:
            if self.stop_flag:
                break

            all_ring_events = self.ring.toArray()
            if len(all_ring_events) > 0:

                # collect updated events
                updated_events: List[PowerBlock] = []
                for e in all_ring_events:
                    if self.update_event_state(e):
                        updated_events.append(e)

                # update cache and db
                if len(updated_events) > 0:
                    # update cache
                    self.redis_cache.update_items(updated_events)

                    # updated_events -> db
                    state_pairs = [(item.timestamp, item.state)
                                   for item in updated_events]
                    self.state_SQL.update_states(state_pairs)

                    # emit to clients
                    pb_updated_json = json.dumps(
                        updated_events, default=pydantic_encoder)
                    self.sio.emit('pb-state-updates', data=pb_updated_json)
                    if config.DEBUG_STATE_UPDATE:
                        print(
                            f'pb-state-updates: ({len(updated_events)} items)')

            # sleep 1 sec
            eventlet.sleep(1)

    def update_event_state(self, e):

        now = utils.datetime_to_ms_since_epoch(datetime.now(tz=timezone.utc))

        state = e.state

        # init -> request
        if state == stateInit:
            if now - e.timestamp < 3000:
                return False  # unmodified
            if now - e.timestamp > 6000:
                return False  # unmodified
            if random.random() < 0.2:
                return False  # unmodified
            e.state = stateRequest
            return True

        # request -> response
        if state == stateRequest:
            if now - e.timestamp < 15000:
                return False  # unmodified
            if now - e.timestamp > 17000:
                return False  # unmodified
            if random.random() < 0.2:
                return False  # unmodified (failed to get response)
            e.state = stateResponse
            return True

        # response -> resolved
        if state == stateResponse:
            if now - e.timestamp < 25000:
                return False  # unmodified
            if now - e.timestamp > 27000:
                return False  # unmodified
            if random.random() < 0.99:
                return False  # unmodified (not resolved)
            e.state = stateResolved
            return True

        if state == stateResolved:
            return False  # unmodified

        return False  # unmodified
