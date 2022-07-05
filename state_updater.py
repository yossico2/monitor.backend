from threading import Timer
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
        self.ring = Ring(ring_size)

    def start(self):
        self.timer = RepeatTimer(interval=1, function=self.on_timer)
        self.timer.setDaemon(True)
        self.timer.start()

    def stop(self):
        if self.timer:
            self.timer.cancel()

    def on_timer(self):
        print(f'on_timer')
        # self.timer.start()

    def on_datagen_events(self, sid: str, events):
        # lilo:TODO
        for e in events:
            e['state'] = stateInit
            self.ring.push(e)
        print(f'on_datagen_events: ({self.ring.count()} events)')
