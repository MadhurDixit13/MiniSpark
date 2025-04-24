import random
class Worker:
    def __init__(self, worker_id):
        self.worker_id = worker_id
        # Could spawn a process/thread here

    def execute(self, partition, transforms):
        data = partition.data
        for func, typ in transforms:
            if typ == 'map':
                data = [func(x) for x in data]
            elif typ == 'filter':
                data = [x for x in data if func(x)]
        return data
    def stop(self):
        pass  # Clean up resources

"""
    Worker process loop: pulls tasks from queue, applies transforms, pushes results.
"""
def worker_loop(task_queue, result_queue, worker_id):
    while True:
        task = task_queue.get()
        if task is None:
            break
        data, transforms = task
        for func, typ in transforms:
            if typ == 'map':
                data = [func(x) for x in data]
            elif typ == 'filter':
                data = [x for x in data if func(x)]
            elif typ == 'flatMap':
                new_data = []
                for x in data:
                    new_data.extend(func(x))
                data = new_data
            elif typ == 'sample':
                frac = func
                data = [x for x in data if random.random() < frac]
        # 'reduceByKey' is handled by scheduler
        result_queue.put(data)