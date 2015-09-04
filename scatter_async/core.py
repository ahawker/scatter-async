"""
    scatter_async.core
    ~~~~~~~~~~~~~~~~~~

    Implements general async object types used regardless of implementation.
"""
__all__ = ('spin_wait', 'Request', 'Job', 'ExecutionState', 'AsyncResult')


from scatter.meta import resolve_callable
from scatter.structures import Enum


ExecutionState = Enum('ExecutionState', 'Pending Queued Running Completed Cancelled Stopped')
JobState = Enum('JobState', 'New Queued Running Completed Stopped')

SPIN_INTERVAL = 0.5


def spin_wait(func, timeout, predicate, interval=SPIN_INTERVAL):
    """
    Given a callable which takes a wait interval, wait for the callable
    to fulfill the predicate function before reaching the timeout.

    :param func: Callable which takes a numeric `interval` value to wait on.
    :param timeout: Numeric value which is the maximum about of time to spin and wait.
    :param predicate: Callable which takes the return value of `func` to check for success.
    :param interval: Numeric value passed to `func` which defines how long it should delay.
    """
    duration = 0
    while True:
        done = predicate(func(interval))
        if done:
            return True

        duration += interval
        if timeout is not None and duration >= timeout:
            return False


class Request(object):
    """

    """

    __slots__ = ('future_id', 'func', 'args', 'kwargs', 'pool', 'seconds')

    def __init__(self, future_id, func, args, kwargs, pool, seconds=None):
        self.future_id = future_id
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.pool = pool
        self.seconds = seconds


class Response(object):
    """
    """

    __slots__ = ('service_id', 'future_id', 'result', 'state', 'msg')

    def __init__(self, service_id, future_id=None, result=None, state=None, msg=None):
        self.service_id = service_id
        self.future_id = future_id
        self.result = result
        self.state = state
        self.msg = msg


class Context(object):
    """
    """

    def __init__(self, service, request):
        self.service_id = service.id
        self.future_id = request.future_id
        self.execution_context = service.type
        self.seconds = request.seconds


class Job(object):
    """
    """

    __slots__ = ('ctx', 'name', 'queue', 'func', 'args', 'kwargs')

    def __init__(self, service, request, pool):
        self.ctx = Context(service, request)
        self.name = resolve_callable(request.func)
        self.queue = pool.queue
        self.func = request.func
        self.args = request.args
        self.kwargs = request.kwargs


    # def __str__(self):
    #     return str(self.ctx)
    #
    # def __repr__(self):
    #     return repr(self.ctx)

    # def __repr__(self):
    #     cls = self.__class__.__name__
    #     return '<{0} at {1}: {2}>'.format(cls, hex(id(self)), self.name)


class JobResult(object):
    """
    """

    __slots__ = ('worker_id', 'future_id', 'result', 'exc_info')

    def __init__(self, worker_id, future_id, result=None, exc_info=None):
        self.worker_id = worker_id
        self.future_id = future_id
        self.result = result
        self.exc_info = exc_info


class JobContext(object):
    """
    """

    id = None
    service = None
    results = None
    args = None
    kwargs = None

    __slots__ = ('id', 'service', 'results', 'args', 'kwargs')

    def __init__(self, results, scope):
        self.results = results


class AsyncRequest(object):
    pass


class AsyncResponse(object):
    pass


class AsyncResult(object):
    """

    """

    __slots__ = ('_result', '_exc_info')

    def __init__(self):
        self._result = None
        self._exc_info = None

    def set_result(self, result):
        self._result = result

    def set_exception(self, exc_info):
        self._exc_info = exc_info

    def __enter__(self):
        yield self

    def __exit__(self, exc_type, exc_val, exc_tb):
        del self._result, self._exc_info