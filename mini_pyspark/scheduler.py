import multiprocessing, functools, queue
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
            
    def _handle_failed_workers(self):
        """
        If any worker has died (os._exit or crash), clean it up and start a new one
        so we can retry its partition.
        """
        for wid, p in enumerate(self.workers):
            if not p.is_alive():
                try:
                    p.terminate()
                except Exception:
                    pass
                new = multiprocessing.Process(
                    target=worker_loop,
                    args=(self.task_queue, self.result_queue, wid),
                    daemon=True
                )
                new.start()
                self.workers[wid] = new
    
    # def _respawn_worker(self, wid):
    #     """Spawn a new worker to replace worker #wid."""
    #     # Clean up old process if still around
    #     old = self.workers[wid]
    #     if old.is_alive():
    #         old.terminate(); old.join()
    #     # Start fresh
    #     p = multiprocessing.Process(
    #         target=worker_loop,
    #         args=(self.task_queue, self.result_queue, wid),
    #         daemon=True
    #     )
    #     p.start()
    #     self.workers[wid] = p

    def run(self, final_rdd):
        # 1) Plan execution into stages
        stages = final_rdd.context.plan(final_rdd)
        # 2) Get initial partitions from root RDD
        root = final_rdd
        while root.prev:
            root = root.prev
        partitions = [p.data for p in root.get_partitions()]

        # 3) Execute each stage in order
        for stage in stages:
            transforms = stage['transforms']
            if not stage['shuffle']:
                # Narrow stage → dispatch & collect one partition at a time,
                # with timeout/retry in case a worker died.
                new_parts = []
                for data in partitions:
                    while True:
                        self.task_queue.put((data, transforms))
                        try:
                            res = self.result_queue.get(timeout=5)
                        except queue.Empty:
                            # no result → likely a dead worker; respawn and retry
                           self._handle_failed_workers()
                           continue
                        else:
                            new_parts.append(res)
                            break
                partitions = new_parts
            else:
                # Wide (shuffle) stage:
                # First apply the “narrow” half, same timeout/retry logic:
                narrow = [(f, t) for f, t in transforms if t != 'reduceByKey']
                reduce_funcs = [f for f, t in transforms if t == 'reduceByKey']
                func = reduce_funcs[0] if reduce_funcs else None

                intermediate = []
                for data in partitions:
                    while True:
                        self.task_queue.put((data, narrow))
                        try:
                            part = self.result_queue.get(timeout=5)
                        except queue.Empty:
                            self._handle_failed_workers()
                            continue
                        else:
                            intermediate.extend(part)
                            break

                # 3b) Central shuffle & reduceByKey

                # 3b) Perform shuffle & reduce centrally
                grouped = {}
                for k, v in intermediate:
                    grouped.setdefault(k, []).append(v)
                partitions = [[(k, functools.reduce(func, vals))]
                              for k, vals in grouped.items()]

        # 4) Flatten final partitions and return
        return [item for part in partitions for item in part]

    def shutdown(self):
        # graceful stop
        for _ in self.workers: self.task_queue.put(None)
        for p in self.workers:
            p.join(timeout=1)
            if p.is_alive(): p.terminate(); p.join()