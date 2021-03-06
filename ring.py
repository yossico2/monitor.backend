from typing import Dict


class Ring:
    def __init__(self, size, key=None):
        if size <= 0:
            raise ValueError('size should be greater than 0!')

        size = int(size)
        self._size = size
        self._list = [None] * size
        self._start = 0
        self._count = 0
        self.key = key
        self.map = {}

    def isFull(self):
        return self._count == self._size

    def isEmpty(self):
        return 0 == self._count

    def count(self):
        return self._count

    def size(self):
        return self._size

    def last(self):
        end = (self._start + self._count) % self._size
        return self._list[end-1]

    def get(self, key):
        if self.key:
            return self.map.get(key)
        return None

    def _get_timestamp(self, item):
        return getattr(item, self.key)

    def push(self, item: Dict):
        end = (self._start + self._count) % self._size
        self._list[end] = item
        if self.key:
            timestamp = self._get_timestamp(item)
            self.map[timestamp] = item
        if self._count == self._size:
            if self.key:
                timestamp = self._get_timestamp(self._list[self._start])
                self.map.pop(timestamp, None)
            self._start = (self._start + 1) % self._size  # full, overwrite
        else:
            self._count += 1

    def dequeue(self):
        if self.isEmpty:
            return None

        item = self._list[self._start]
        if self.key:
            timestamp = self._get_timestamp(item)
            self.map.pop(timestamp, None)
        self._list[self._start] = None
        self._start = (self._start + 1) % self._size
        self._count -= 1

        return item

    def forEach(self, cb):
        i = self._start
        count = self._count
        while count > 0:
            cb(self._list[i])
            i = (i + 1) % self._size
            count -= 1

    def toArray(self):
        arr = []
        self.forEach(lambda item: arr.append(item))
        return arr
