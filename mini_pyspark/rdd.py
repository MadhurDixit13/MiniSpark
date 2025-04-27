from .partition import Partition

class RDD:
    def __init__(self, context, data=None, num_partitions=None,
                 prev=None, transform=None, ttype=None):
        # store the raw data on the root RDD so get_partitions() can re-slice it
        self.data = data

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
        self.is_cached         = False
        self.cached_partitions = None
        # storage for the final collect() result
        self._cached_data = None

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
    
    def cache(self):
        """Mark this RDD to cache its partitions after first action."""
        self.is_cached = True
        return self
    
    def get_partitions(self):
        """
        Return partitions either from cache (if present)
        or by creating/fetching fresh ones.
        """
        # Return cached data if available
        if self.is_cached and self.cached_partitions is not None:
            return [
                Partition(idx, part_data)
                for idx, part_data in enumerate(self.cached_partitions)
            ]

        # For root RDD, (re)create partitions
        if self.prev is None:
            parts = self._create_partitions(self.data)
        else:
            # For non-root, scheduler will have populated self.partitions
            parts = self.partitions

        # Cache raw lists for reuse
        if self.is_cached:
            self.cached_partitions = [p.data for p in parts]

        return parts

    def collect(self):
        # if we've cached already, just return that
        if self.is_cached and self._cached_data is not None:
            return self._cached_data

        # otherwise run the scheduler…
        result = self.context.scheduler.run(self)

        # …and cache it if requested
        if self.is_cached:
            self._cached_data = result
        return result


    def count(self):
        # count leverages cached collect()
        return len(self.collect())

    
    def take(self, n):
        return self.context.scheduler.take(self, n)  