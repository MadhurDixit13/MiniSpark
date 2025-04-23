import functools
import multiprocessing
from .worker import worker_loop


class TaskScheduler:
    """
    Distributes RDD partitions and transformations to worker processes.
    """
    def __init__(self, num_workers):
        self.num_workers = num_workers
        self.task_queue = multiprocessing.Queue()
        self.result_queue = multiprocessing.Queue()
        self.workers = []
        for wid in range(num_workers):
            p = multiprocessing.Process(
                target=worker_loop,
                args=(self.task_queue, self.result_queue, wid),
                daemon=True
            )
            p.start()
            self.workers.append(p)

    def run(self, rdd):
        # 1) Dispatch each partition + full transform list
        for part in rdd.partitions:
            self.task_queue.put((part.data, rdd.transforms))
        # 2) Signal workers to stop after tasks
        for _ in self.workers:
            self.task_queue.put(None)

        # 3) Gather intermediate results
        intermediate = []
        for _ in rdd.partitions:
            intermediate.extend(self.result_queue.get())

        # 4) If reduceByKey present, perform shuffle & reduce
        reduce_funcs = [f for f, t in rdd.transforms if t == 'reduceByKey']
        if reduce_funcs:
            func = reduce_funcs[0]
            grouped = {}
            for key, val in intermediate:
                grouped.setdefault(key, []).append(val)
            return [(k, functools.reduce(func, vals)) for k, vals in grouped.items()]

        # 5) Otherwise, return raw intermediate list
        return intermediate

    def shutdown(self):
        # Ensure processes finish
        for p in self.workers:
            p.join()