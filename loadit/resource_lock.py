from contextlib import contextmanager


class ResourceLock(object):

    def __init__(self, main_lock, locks, locked_resources):
        self.main_lock = main_lock
        self.locks = locks
        self.locked_resources = locked_resources

    @contextmanager
    def acquire(self, resource, block=True):

        with self.main_lock:
            locked_resources = self.locked_resources._getvalue()

            try:
                lock_index, queue, n_jobs = locked_resources[resource]
            except KeyError:
                used_locks = {i for i, _ in locked_resources.values()}
                lock_index = [i for i in range(len(self.locks)) if i not in used_locks].pop()
                queue = 0
                n_jobs = 0

            self.locked_resources[resource] = (lock_index, queue + 1, n_jobs)
            lock = self.locks[lock_index]

        lock.acquire()

        with self.main_lock:
            lock_index, queue, n_jobs = self.locked_resources[resource]
            self.locked_resources[resource] = (lock_index, queue, n_jobs + 1)

        if block:

            while True:

                with self.main_lock:

                    if self.locked_resources[resource][2] == 1:
                        break

                time.sleep(1)
        else:
            lock.release()

        try:
            yield
        finally: # Release resource

            with self.main_lock:
                lock_index, queue, n_jobs = self.locked_resources[resource]

                if queue > 1:
                    self.locked_resources[resource] = (lock_index, queue - 1, n_jobs - 1)
                else:
                    del self.locked_resources[resource]

                if block:
                    lock.release()