import sys
import threading
from six.moves import queue as Queue
import logging
import time

class ManagerStoppedException(Exception):
    pass

class Task(object):
    def __init__(self, **kw):
        self._wait_event = threading.Event()
        self._exception = None
        self._traceback = None
        self._result = None
        self.args = kw

    def done(self, result):
        self._result = result
        self._wait_event.set()

    def exception(self, e, tb):
        self._exception = e
        self._traceback = tb
        self._wait_event.set()

    def wait(self):
        self._wait_event.wait()
        if self._exception:
            raise self._exception, None, self._traceback
        return self._result

    def run(self, client):
        try:
            self.done(self.main(client))
        except Exception as e:
            self.exception(e, sys.exc_info()[2])

class TaskManager(threading.Thread):
    log = logging.getLogger("nodepool.TaskManager")

    def __init__(self, client):
        super(TaskManager, self).__init__()
        self.daemon = True
        self.queue = Queue.Queue()
        self._running = True
        self._client = None

    def stop(self):
        self._running = False
        self.queue.put(None)

    def run(self):
        last_ts = 0
        while True:
            task = self.queue.get()
            if not task:
                if not self._running:
                    break
                continue
            while True:
                delta = time.time() - last_ts
                if delta >= self.rate:
                    break
                time.sleep(self.rate - delta)
            self.log.debug("Manager running task %s (queue: %s)" %
                           (task, self.queue.qsize()))
            start = time.time()
            task.run(self._client)
            last_ts = time.time()
            self.log.debug("Manager ran task %s in %ss" %
                           (task, (last_ts - start)))
            self.queue.task_done()

    def submitTask(self, task):
        if not self._running:
            raise ManagerStoppedException(
                "Manager is no longer running")
        self.queue.put(task)
        return task.wait()
