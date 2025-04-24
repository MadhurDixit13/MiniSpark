import multiprocessing, functools
from .worker import worker_loop

class TaskScheduler:
    """Executes each stage: narrow funcs via workers, shuffle stages centrally."""
    def __init__(self, num_workers):
        self.num_workers = num_workers
        self.task_queue = multiprocessing.Queue()
        self.result_queue = multiprocessing.Queue()
        self.workers = []
        for wid in range(num_workers):
            p = multiprocessing.Process(target=worker_loop,
                                        args=(self.task_queue, self.result_queue, wid),
                                        daemon=True)
            p.start(); self.workers.append(p)

    def run(self, final_rdd):
        # 1) Plan execution into stages
        stages = final_rdd.context.plan(final_rdd)
        # 2) Get initial partitions from root RDD
        root = final_rdd
        while root.prev:
            root = root.prev
        partitions = [p.data for p in root.partitions]

        # 3) Execute each stage in order
        for stage in stages:
            transforms = stage['transforms']
            if not stage['shuffle']:
                # Narrow stage: apply all transforms directly
                tasks = partitions
                partitions = []
                for data in tasks:
                    self.task_queue.put((data, transforms))
                for _ in tasks:
                    partitions.append(self.result_queue.get())
            else:
                # Shuffle stage: separate narrow transforms and the reduceByKey
                narrow = [(f, t) for f, t in transforms if t != 'reduceByKey']
                reduce_funcs = [f for f, t in transforms if t == 'reduceByKey']
                func = reduce_funcs[0] if reduce_funcs else None

                # 3a) Apply narrow transforms on each partition
                intermediate = []
                for data in partitions:
                    self.task_queue.put((data, narrow))
                for _ in partitions:
                    intermediate.extend(self.result_queue.get())

                # 3b) Perform shuffle & reduce centrally
                grouped = {}
                for k, v in intermediate:
                    grouped.setdefault(k, []).append(v)
                partitions = [[(k, functools.reduce(func, vals))]
                              for k, vals in grouped.items()]

        # 4) Flatten final partitions
        return [item for part in partitions for item in part]

    def shutdown(self):
        # graceful stop
        for _ in self.workers: self.task_queue.put(None)
        for p in self.workers:
            p.join(timeout=1)
            if p.is_alive(): p.terminate(); p.join()