import pickle
from .rdd import RDD
from .scheduler import TaskScheduler
from .planner import create_stages
from .viz import visualize

class MiniSparkContext:
    """
    Entry point for the mini Spark cluster.
    """
    def __init__(self, num_workers=2):
        self.num_workers = num_workers
        self.scheduler = TaskScheduler(num_workers)

    def parallelize(self, data, num_partitions=None):
        """
        Split data into partitions and wrap into an RDD.
        """
        return RDD(self, data, num_partitions)

    def plan(self, rdd):
        """
        Build execution stages from RDD lineage.
        """
        return create_stages(rdd)
    
    def visualize(self, rdd):
        """Draw the DAG of RDD transformations"""
        visualize(rdd)

    def shutdown(self):
        """
        Clean up worker processes.
        """
        self.scheduler.shutdown()