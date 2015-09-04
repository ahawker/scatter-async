"""
    scatter_async.queue
    ~~~~~~~~~~~~~~~~~~~

"""
__all__ = ('Queue', 'QueueService')


from scatter.config import ConfigAttribute
from scatter.descriptors import cached
from scatter.service import Service


class QueueEmpty(Exception):
    """

    """


class QueueFull(Exception):
    """

    """


class Queue(object):
    """

    """

    queue_cls = None

    def __init__(self, *args, **kwargs):
        self._queue = self.queue_cls(*args, **kwargs)

    def get(self, block=True, timeout=None):
        """
        """
        return self._queue.get(block, timeout)

    def get_nowait(self):
        """
        """
        return self._queue.get_nowait()

    def put(self, obj, block=True, timeout=None):
        """
        """
        return self._queue.put(obj, block, timeout)

    def put_nowait(self, obj):
        """
        """
        return self._queue.put_nowait(obj)

    def join(self, timeout=None):
        """
        """
        return self._queue.join(timeout)

    def qsize(self):
        """
        """
        return self._queue.qsize()

    def full(self):
        """
        """
        return self._queue.full()

    def empty(self):
        """
        """
        return self._queue.empty()

    def task_done(self):
        """
        """
        return self._queue.task_done()


class QueueService(Service):
    """

    """

    __abstract__ = True

    #:
    #:
    poison_pill = object()

    #:
    #:
    queue_class = ConfigAttribute(Queue)

    #:
    #:
    queue_max_size = ConfigAttribute()

    #:
    #:
    queue_empty_exception = ConfigAttribute()

    #:
    #:
    queue_full_exception = ConfigAttribute()

    @cached
    def queue(self):
        return self.queue_class(maxsize=self.queue_max_size)

    def get(self, block=True, timeout=None):
        """
        """
        return self.queue.get(block, timeout)

    def get_nowait(self):
        """
        """
        try:
            return self.queue.get_nowait()
        except self.queue_empty_exception:
            raise QueueEmpty

    def put(self, obj, block=True, timeout=None):
        """
        """
        return self.queue.put(obj, block, timeout)

    def put_nowait(self, obj):
        """
        """
        try:
            return self.queue.put_nowait(obj)
        except self.queue_full_exception:
            raise QueueFull

    def join(self, timeout=None):
        """
        """
        return self.queue.join(timeout)

    def qsize(self):
        """
        """
        return self.queue.qsize()

    def full(self):
        """
        """
        return self.queue.full()

    def empty(self):
        """
        """
        return self.queue.empty()

    def task_done(self):
        """
        """
        return self.queue.task_done()

    def notify_stop(self):
        """

        """
        return self.queue.put(self.poison_pill)
