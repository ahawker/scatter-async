"""
    scatter_async.worker
    ~~~~~~~~~~~~~~~~~~~~

"""
__all__ = ('Worker', 'WorkerService')


import functools
import sys

from scatter.config import ConfigAttribute
from scatter.descriptors import cached
from scatter.exceptions import ScatterExit, ScatterTimeout
from scatter.meta import resolve_callable
from scatter.service import Service, initialized
from scatter_async.core import AsyncResult
from scatter_async.queue import QueueService


class Worker(object):
    """
    """

    #:
    #:
    worker_cls = None

    def __init__(self, func, *args, **kwargs):
        self._worker = self.worker_cls(self.binder(func), *args, **kwargs)

    @classmethod
    def spawn(cls, func, *args, **kwargs):
        """
        """
        worker = cls(func, *args, **kwargs)
        worker.start()
        return worker

    @classmethod
    def spawn_later(cls, seconds, func, *args, **kwargs):
        """
        """
        worker = cls(func, *args, **kwargs)
        worker.start_later(seconds)
        return worker

    # @classmethod
    # def spawn_pool(cls, func, *args, **kwargs):
    #     """
    #     """
    #     worker = cls(func, *args, **kwargs)
    #     worker.start()
    #     return worker
    #
    # @classmethod
    # def spawn_pool_later(cls, seconds, results, func, *args, **kwargs):
    #     """
    #     """
    #     worker = cls(results, func, *args, **kwargs)
    #     worker.start_later(seconds)
    #     return worker

    @property
    def id(self):
        return self._worker.id

    @property
    def name(self):
        return self._worker.name

    @name.setter
    def name(self, value):
        self._worker.name = value

    # @property
    # def queue(self):
    #     return self._worker.queue
    #
    # @queue.setter
    # def queue(self, value):
    #     self._worker.queue = value

    def start(self):
        """
        """
        return self._worker.start()

    def start_later(self, seconds):
        """
        """
        return self._worker.start_later(seconds)

    def started(self):
        """
        """
        return self._worker.started()

    def running(self):
        """
        """
        return self._worker.running()

    def completed(self):
        """
        """
        return self._worker.completed()

    def wait_for_start(self, timeout=None):
        """
        """
        return self._worker.wait_for_start(timeout)

    def wait_for_completion(self, timeout=None):
        """
        """
        return self._worker.wait_for_completion(timeout)

    def cancel(self):
        """
        """
        return self._worker.cancel()

    def stop(self, exception=ScatterExit, block=True, timeout=None):
        """
        """
        return self._worker.stop(exception, block, timeout)

    def join(self, timeout=None):
        """
        """
        return self._worker.join(timeout)

    # def get(self, block=True, timeout=None):
    #     """
    #     """
    #     if self._result:
    #         return self._result
    #
    #     result = self._worker_result.get(block, timeout)
    #     if not result.completed():
    #         raise ScatterTimeout('No worker result after timeout of {0} seconds'.format(timeout))
    #
    #     self._result = result
    #     if result.exc_info:
    #         raise result.exc_info
    #     return result.result
    #
    # def set(self, result):
    #     """
    #     """
    #     if self._results is not None:
    #         self._results.put(result)
    #     return self._worker_result.put(result)

    def is_current(self):
        """
        """
        return self._worker.is_current()

    def binder(self, func):
        """
        """
        @functools.wraps(func)
        def bound(*args, **kwargs):
            return func(self, *args, **kwargs)
        return bound


class WorkerService(Service):
    """
    """

    #:
    #:
    __abstract__ = True

    #:
    #:
    worker_class = None

    @classmethod
    def spawn(cls, func, *args, **kwargs):
        return cls.worker_class.spawn(func, *args, **kwargs)

    @classmethod
    def spawn_later(cls, seconds, func, *args, **kwargs):
        return cls.worker_class.spawn_later(seconds, func, *args, **kwargs)

    @classmethod
    def run(cls, worker, func, *args, **kwargs):
        with cls.new() as service:
            return func(service, worker, *args, **kwargs)

    #
    # #:
    # #:
    # worker = None
    #
    # #:
    # #:
    # worker_class = ConfigAttribute()
    #
    # #:
    # #:
    # detach_on_stop = ConfigAttribute(True)
    #
    # #:
    # #:
    # result_class = ConfigAttribute(AsyncResult)
    #
    # #:
    # #:
    # result_queue_class = ConfigAttribute(QueueService)
    #
    # #service has a future object
    # #passes it to worker_class instance
    # #passes to execution context
    # #
    #
    # @property
    # def results(self):
    #     return self.parent.results
    #
    # @classmethod
    # def hawker(cls, func, *args, **kwargs):
    #     pass
    #
    # @initialized
    # def spawn(self, func, *args, **kwargs):
    #     """
    #     """
    #     worker = self.worker = self.worker_class.spawn(self.call, func, *args, **kwargs)
    #     worker.name = self.name
    #     return worker
    #
    # @initialized
    # def spawn_later(self, seconds, func, *args, **kwargs):
    #     """
    #     """
    #     worker = self.worker = self.worker_class.spawn_later(seconds, self.call, func, *args, **kwargs)
    #     worker.name = self.name
    #     return worker
    #
    # @initialized
    # def spawn_pool(self, func, *args, **kwargs):
    #     """
    #     """
    #     worker = self.worker = self.worker_class.spawn_pool(self.results, self.call, func, *args, **kwargs)
    #     worker.name = self.name
    #     return worker
    #
    # @initialized
    # def spawn_pool_later(self, seconds, func, *args, **kwargs):
    #     """
    #     """
    #     worker = self.worker = self.worker_class.spawn_pool_later(seconds, self.results, self.call, func, *args, **kwargs)
    #     worker.name = self.name
    #     return worker
    #
    # def run(self, *args, **kwargs):
    #     pass
    #
    # @staticmethod
    # def call(channel, ctx, func, *args, **kwargs):
    #     """
    #     """
    #     # create a process service
    #     # create a worker service
    #     # run this shit
    #     try:
    #         while True:
    #             try:
    #                 pass
    #             finally:
    #                 pass
    #     except Exception:
    #         pass
    #     finally:
    #         pass
    #
    #
    # # def call(self, worker, func, *args, **kwargs):
    # #     """
    # #     """
    # #     with self:
    # #         self.log.info('Worker call {0}'.format(resolve_callable(func)))
    # #         with AsyncResult(self, worker) as result:
    # #             try:
    # #                 result.result = func(self, *args, **kwargs)
    # #             except ScatterExit:
    # #                 self.log.info('Forcefully stopped with ScatterExit.')
    # #             except Exception:
    # #                 result.exc_info = sys.exc_info()
    # #             finally:
    # #                 worker.set(result)
    #
    # def on_stopped(self, *args, **kwargs):
    #     """
    #     """
    #     # Normally, a worker service will be stopped by its own worker.
    #     # If the service is stopped by a different caller, we must wait and forcefully
    #     # stop the underlying worker if necessary.
    #     if not self.worker.completed() and not self.worker.is_current():
    #         self.log.info('Worker {0} is still running, waiting for it to finish.'.format(self.worker))
    #         timeout = self.stop_timeout
    #         stopped = self.worker.join(timeout)
    #         if not stopped:
    #             self.log.info('Worker {0} is still running, attempting to forcefully stop it.'.format(self.worker))
    #             stopped = self.worker.stop(timeout=timeout)
    #             if not stopped:
    #                 msg = 'Worker {0} is STILL running, failed to stop it after {1} seconds.'
    #                 self.log.warning(msg.format(self.worker, timeout))
    #
    #     # Automatically detach worker services from their parent pool as their lifecycle
    #     # lasts only as long as the underlying worker's execution time.
    #     if self.detach_on_stop and self.parent:
    #         self.parent.detach(self)


#
# class WorkerService(Service):
#     """
#
#     """
#
#     #:
#     #:
#     __abstract__ = True
#
#     #:
#     #:
#     worker = None
#
#     #:
#     #:
#     worker_class = ConfigAttribute()
#
#     #:
#     #:
#     detach_on_stop = ConfigAttribute(True)
#
#     # #:
#     # #:
#     # max_tasks = ConfigAttribute()
#     #
#     # #:
#     # #:
#     # idle_ttl = ConfigAttribute()
#
#     # @cached
#     # def messages(self):
#     #     return self.services.by_name('messages')
#
#     # @classmethod
#     # def spawn(cls, func, *args, **kwargs):
#     #     """
#     #     """
#     #     worker = cls()
#     #     worker.start(func, *args, **kwargs)
#     #     return worker
#     #
#     # @classmethod
#     # def spawn_later(cls, seconds, func, *args, **kwargs):
#     #     """
#     #     """
#     #     worker = cls()
#     #     pass
#
#     #@initialized
#     def spawn(self, job):
#         """
#         """
#         self.worker = self.worker_class.spawn(self.call, job.queue, job.func, *job.args, **job.kwargs)
#         self.worker.name = self.name
#
#     #@initialized
#     def spawn_later(self, seconds, job):
#         """
#         """
#         self.worker = self.worker_class.spawn_later(seconds, self.call, job.queue, job.func, *job.args, **job.kwargs)
#         self.worker.name = self.name
#
#     def call(self, queue, func, *args, **kwargs):
#         """
#         Call this function with the given arguments as if it were a bound method
#         on this service instance.
#
#         :param func: Function to call as a bound method
#         :param args: (Optional) function arguments
#         :param kwargs: (Optional) function keyword arguments
#         """
#         with self:
#             with AsyncResult() as result:
#                 try:
#                     result.result = func(self, *args, **kwargs)
#                 except ScatterExit:
#                     self.log.info('Forcefully stopped with ScatterExit.')
#                 except Exception:
#                     result.exc_info = sys.exc_info()
#                 finally:
#                     queue.put(result)
#
#     def on_stopped(self, *args, **kwargs):
#         """
#
#         """
#         # Normally, a worker service will be stopped by its own worker.
#         # If the service is stopped by a different caller, we must wait and forcefully
#         # stop the underlying worker if necessary.
#         if not self.worker.is_current():
#             self.log.info('Worker {0} is still running, waiting for it to finish.'.format(self.worker))
#             timeout = self.stop_timeout
#             stopped = self.worker.join(timeout)
#             if not stopped:
#                 self.log.info('Worker {0} is still running, attempting to forcefully stop it.'.format(self.worker))
#                 stopped = self.worker.stop(timeout=timeout)
#                 if not stopped:
#                     msg = 'Worker {0} is STILL running, failed to stop it after {1} seconds.'
#                     self.log.warning(msg.format(self.worker, timeout))
#
#         # Automatically detach worker services from their parent pool as their lifecycle
#         # lasts only as long as the underlying worker's execution time.
#         if self.detach_on_stop and self.parent:
#             self.parent.detach(self)
