from .worker import Worker

class TaskScheduler:
    def __init__(self, num_workers):
        self.num_workers = num_workers
        self.workers = [Worker(i) for i in range(num_workers)]

    def run(self, rdd):
        """
        Distribute RDD partitions and transformations to workers.
        """
        tasks = []
        for partition in rdd.partitions:
            tasks.append((partition, rdd.transforms))
        # Simple round-robin dispatch
        results = []
        for idx, (part, transforms) in enumerate(tasks):
            worker = self.workers[idx % self.num_workers]
            results.extend(worker.execute(part, transforms))
        return results

    def shutdown(self):
        for w in self.workers:
            w.stop()