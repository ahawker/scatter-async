"""
    scatter_async.future
    ~~~~~~~~~~~~~~~~~~~~

    Implements futures, objects which contain a result to be populated in the future from
    an asynchronous worker.
"""
__all__ = ('Future', 'FutureService', 'FutureState', 'FutureProxy')


import collections
import itertools

from scatter.config import ConfigAttribute
from scatter.descriptors import cached
from scatter.exceptions import ScatterException, ScatterCancel, ScatterTimeout
from scatter.service import Service, running
from scatter.structures import Enum
from scatter.structures import ScatterDict
from scatter_async.core import ExecutionState


FutureState = Enum('FutureState', 'Pending Queued Running Completed Cancelled Stopped')


class FutureException(ScatterException):
    """

    """


class ScatterFuture(object):
    """
    """

    #:
    #:
    event_cls = None

    def __init__(self):
        self._callbacks = collections.deque()
        self._complete = self.event_cls()
        self._result = None
        self._exc_info = None
        self._state = ExecutionState.Pending

    def cancelled(self):
        """
        """
        return self._state == ExecutionState.Cancelled

    def running(self):
        """
        """
        return self._state == ExecutionState.Running

    def completed(self):
        """
        """
        return self._state == ExecutionState.Completed

    def successful(self):
        """
        """
        return self.completed() and self._exc_info is None

    def cancel(self):
        """
        """
        if self._state != ExecutionState.Pending:
            return False
        self._state = ExecutionState.Cancelled
        self._complete.set()

    def get(self, block=True, timeout=None):
        """
        """
        # Future was previously cancelled by user, raise immediately.
        if self.cancelled():
            raise ScatterCancel('Future has been cancelled')

        # Future already finished, return/raise its cached result/exception.
        if self.completed():
            if self._exc_info:
                raise self._exc_info
            return self._result

        # Raise timeout when user doesn't want to block and we don't have a result yet.
        if not block:
            raise ScatterTimeout('Future not ready')

        # Wait for specified timeout, raising exception if we still don't have a result.
        complete = self._complete.wait(timeout)
        if not complete:
            raise ScatterTimeout('Future not ready after {0} seconds'.format(timeout))

        # Future marked completed due to being cancelled.
        if self.cancelled():
            raise ScatterCancel('Future has been cancelled')

        # Finished without cancellation, return or raise.
        if self._exc_info:
            raise self._exc_info
        return self._result

    def add_callback(self, callback):
        """
        """
        if self.completed():
            raise FutureException('Cannot add callbacks to a completed future')
        self._callbacks.append(callback)

    def remove_callback(self, callback):
        """
        """
        self._callbacks.remove(callback)

    def has_callbacks(self):
        """
        """
        return len(self._callbacks) > 0

    def set_result(self, result):
        """
        """
        if self._complete.is_set():
            raise FutureException('Future value already set')

        self._result = result
        self._state = ExecutionState.Completed
        self._complete.set()

    def set_exception(self, exc_info):
        """
        """
        if self._complete.is_set():
            raise FutureException('Future value already set')

        self._exc_info = exc_info
        self._state = ExecutionState.Completed
        self._complete.set()

    def set_state(self, state):
        """
        """
        self._state = state


class Future(object):
    """

    """

    #:
    #:
    future_cls = None

    #:
    #:
    lock_cls = None

    def __init__(self, future_id, *args, **kwargs):
        self._id = future_id
        #self._future = self.future_cls(*args, **kwargs)
        self._state = ExecutionState.Pending

    @property
    def id(self):
        return self._id

    @property
    def state(self):
        return self._state

    def get(self, block=True, timeout=None):
        """

        """
        return self._future.get(block, timeout)

    def get_nowait(self):
        """
        """
        return self.get(block=False)

    def completed(self):
        """

        """
        return self._future.completed()

    def successful(self):
        """
        """
        return self._future.successful()

    def link(self, func):
        """

        """
        return self._future.add_callback(func)

    def unlink(self, func):
        """

        """
        return self._future.remove_callback(func)

    def set_result(self, result):
        """

        """
        return self._future.set_result(result)

    def set_exception(self, exc_info):
        """

        """
        return self._future.set_exception(exc_info)

    def set_state(self, state):
        """
        """
        self._state = state

    def set_from_response(self, response):
        """
        Set the future state and potential result/exception from the given response.
        """
        if response is None:
            raise ValueError('response cannot be None')

        if response.completed():
            if response.exc_info:
                self.set_exception(response.exc_info)
            else:
                self.set_result(response.result)
        else:
            self.set_state(response.state)


class FutureProxy(object):
    pass


class FutureCollection(ScatterDict):
    """

    """
    #needs to be weakref store

    def __init__(self, future_cls, *args, **kwargs):
        super(FutureCollection, self).__init__(*args, **kwargs)
        self.future_cls = future_cls
        self.counter = itertools.count()

    def next(self):
        """

        """
        future_id = self.counter.next()
        future = self[future_id] = self.future_cls(future_id)
        return future


class FutureService(Service):
    """

    """

    __abstract__ = True

    #:
    #:
    future_class = ConfigAttribute(Future)

    #:
    #:
    future_collection_class = ConfigAttribute(FutureCollection)

    @cached
    def futures(self):
        return self.future_collection_class(self.future_class)

    #@running
    def next(self):
        """
        """
        return self.futures.next()

    #@running
    def get(self, future_id, default=None):
        """

        """
        return self.futures.get(future_id, default)

