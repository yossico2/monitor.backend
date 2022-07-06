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

    def push(self, item):
        end = (self._start + self._count) % self._size
        self._list[end] = item
        if self.key:
            self.key.set(item[self.key], item)
        if self._count == self._size:
            if self.key:
                self.map.pop(self._list[self.start][self.key], None)
            self._start = (self._start + 1) % self._size  # full, overwrite
        else:
            self._count += 1

    def dequeue(self):
        if self.isEmpty:
            return None

        item = self._list[self._start]
        if self.key:
            self.map.pop(item[self.key], None)
        self._list[self._start] = None
        self._start = (self._start + 1) % self._size
        self._count -= 1

        return item

    def foreach(self, cb):
        for item in self._list:
            cb(item)

    def toArray(self):
        arr = []
        for item in self._list:
            arr.append(item)
        return arr
