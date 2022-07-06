import random
from datetime import datetime, timezone
from threading import Timer

import utils
from ring import Ring

# constants
# -------------------
SEC = 1000  # ms
MINUTE = 60 * SEC
PERIOD = 100  # ms

stateInit = 0
stateRequest = 1
stateResponse = 2
stateResolved = 3


class RepeatTimer(Timer):
    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)


class StateUpdater:
    def __init__(self):
        ring_size = 5 * MINUTE / PERIOD
        self.ring = Ring(ring_size, key='timestamp')

    def start(self):
        self.timer = RepeatTimer(interval=1, function=self.update_events_state)
        self.timer.setDaemon(True)
        self.timer.start()

    def stop(self):
        if self.timer:
            self.timer.cancel()

    def on_datagen_events(self, sid: str, events):
        for e in events:
            e['state'] = stateInit
            self.ring.push(e)
        print(f'on_datagen_events: ({self.ring.count()} events)')

    def update_events_state(self):
        print(f'update_events_state')
        events = self.ring.toArray()
        updated = []
        for e in events:
            if self.update_event_state(e):
                updated.append(e)
        
        # update cache
        # lilo:TODO

        # update db
        # lilo:TODO

        # emit to clients
        self.sio.emit('pb-events-state', data=updated)

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
            if now - timestamp.getTime() > 7000:
                return False  # unmodified
            if random.random() < 0.2:
                return False  # unmodified (failed to get response)
            e['state'] = stateResponse
            return True

        if state == stateResponse:
            if now - timestamp.getTime() > 7000:
                return False  # unmodified
            if random.random() < 0.95:
                return False  # unmodified (not resolved)
            e['state'] = stateResolved
            return True

        if state == stateResolved:
            return False  # unmodified

        return False  # unmodified
