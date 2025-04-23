import pickle
from .rdd import RDD
from .scheduler import TaskScheduler

class MiniSparkContext:
    """
    Entry point for the mini Spark cluster.
    """
    def __init__(self, num_workers=2):
        self.scheduler = TaskScheduler(num_workers)

    def parallelize(self, data, num_partitions=None):
        """
        Split data into partitions and wrap into an RDD.
        """

    def shutdown(self):
        """
        Clean up worker processes.
        """