from .partition import Partition

class RDD:
    def __init__(self, context, data, num_partitions=None):
        self.context = context
        self.num_partitions = num_partitions or context.scheduler.num_workers
        self.partitions = self._create_partitions(data)
        self.transforms = []  # store (func, type) for lazy evaluation

    def _create_partitions(self, data):
        slice_size = len(data) // self.num_partitions
        parts = []
        for i in range(self.num_partitions):
            start = i * slice_size
            end = None if i == self.num_partitions - 1 else (i+1) * slice_size
            parts.append(Partition(i, data[start:end]))
        return parts

    def map(self, func):
        self.transforms.append((func, 'map'))
        return self

    def filter(self, func):
        self.transforms.append((func, 'filter'))
        return self

    def collect(self):
        # Trigger execution: send tasks to scheduler
        return self.context.scheduler.run(self)
    
    def reduceByKey(self, func):
        """Add a reduceByKey action (lazy)"""
        self.transforms.append((func, 'reduceByKey'))
        return self
