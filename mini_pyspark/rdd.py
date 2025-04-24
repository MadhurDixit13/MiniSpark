from .partition import Partition

class RDD:
    def __init__(self, context, data=None, num_partitions=None,
                 prev=None, transform=None, ttype=None):
        self.context = context
        # Root RDD holds actual data
        if prev is None:
            self.num_partitions = num_partitions or context.num_workers
            self.partitions = self._create_partitions(data)
        else:
            # Non-root: partitions filled by scheduler after shuffle stage
            self.num_partitions = None
            self.partitions = None
        # Lineage pointers
        self.prev = prev
        self.transform = transform  # function or param
        self.ttype = ttype          # 'map','filter','flatMap','sample','reduceByKey'

    def _create_partitions(self, data):
        slice_size = max(len(data) // self.num_partitions, 1)
        return [Partition(i, data[i*slice_size : None if i+1==self.num_partitions else (i+1)*slice_size])
                for i in range(self.num_partitions)]

    def _new(self, transform, ttype):
        # Create a new RDD node in the lineage
        return RDD(self.context, prev=self, transform=transform, ttype=ttype)

    def map(self, func):      return self._new(func, 'map')
    def filter(self, func):   return self._new(func, 'filter')
    def flatMap(self, func):  return self._new(func, 'flatMap')
    def sample(self, frac):   return self._new(frac, 'sample')
    def reduceByKey(self, func): return self._new(func, 'reduceByKey')

    def collect(self):
        return self.context.scheduler.run(self)

    def count(self):
        return len(self.collect())