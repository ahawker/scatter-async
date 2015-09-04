"""
    scatter_async.pool
    ~~~~~~~~~~~~~~~~~~

"""
__all__ = ('Pool', 'PoolService', 'GreenletPoolService')


import abc
import functools
import itertools
import sys
import traceback

from scatter.config import ConfigAttribute
from scatter.exceptions import ScatterExit
from scatter.meta import resolve_class, resolve_callable
from scatter.service import Service, running, initialized, DependencyAttribute
from scatter_async.core import Request, Response, AsyncResult, ExecutionState
from scatter_async.future import FutureService, FutureState
from scatter_async.queue import QueueService
from scatter_async.worker import WorkerService


class Pool(object):
    """

    """

    pool_cls = None

    def __init__(self, *args, **kwargs):
        self._pool = self.pool_cls(*args, **kwargs)

    def __len__(self):
        return len(self._pool)

    def __contains__(self, item):
        return item in self._pool

    def __iter__(self):
        return iter(self._pool)

    @property
    def is_full(self):
        return len(self) == self.size

    @property
    def name(self):
        return resolve_class(self)

    @abc.abstractproperty
    def size(self):
        raise NotImplementedError('Abstract property must be implemented in derived class.')

    def spawn(self, func, *args, **kwargs):
        """
        """
        return self._pool.spawn(func, *args, **kwargs)

    def spawn_later(self, seconds, func, *args, **kwargs):
        """
        """
        return self._pool.spawn_later(seconds, func, *args, **kwargs)

    @abc.abstractmethod
    def stop(self, exception=ScatterExit, block=True, timeout=None):
        raise NotImplementedError('Abstract method must be implemented in derived class.')

    @abc.abstractmethod
    def join(self, timeout=None):
        raise NotImplementedError('Abstract method must be implemented in derived class.')

    @abc.abstractmethod
    def resize(self, size):
        raise NotImplementedError('Abstract method must be implemented in derived class.')


class PoolWorker(Service):
    """
    """

    # a pool worker has a channel
    # a pool worker spawns a worker and feeds it the channel and execution ctx (settings)
    # a pool worker holds onto the worker instance
    # a pool worker, when told to stop by the pool, tells the remote worker to stop and waits.

    __abstract__ = True

    #:
    #:
    worker = None

    #:
    #:
    worker_class = ConfigAttribute()

    def on_started(self, *args, **kwargs):
        """
        """
        self.worker = self.worker_class.spawn(self.worker_class.run, pool_worker, *args, **kwargs)
        self.worker.name = self.name

    def on_stopped(self, *args, **kwargs):
        """
        """
        # Normally, a pool worker is stopped when its worker has notified the channel
        # that is is stopping. The pool will receive this message and tell the pool worker to stop.
        # The other case is when the pool tells the pool worker to stop because the pool itself is stopping.
        # As part of the pool stop process, it will notify its channels to stop, which should be picked
        # up by the workers. The pool worker responsibility here is to make sure its worker stops,
        # forcing it if necessary.
        if not self.worker.completed():
            self.log.info('Worker {0} is still running, waiting for it to finish.'.format(self.worker))
            timeout = self.stop_timeout
            stopped = self.worker.join(timeout)
            if not stopped:
                self.log.info('Worker {0} is still running, attempting to forcefully stop it'.format(self.worker))
                stopped = self.worker.stop(timeout=timeout)
                if not stopped:
                    msg = 'Worker {0} is still running, failed to stop it after {1} seconds'
                    self.log.warning(msg.format(self.worker, timeout))


def pool_worker(service, requests, responses):
    """

    """
    try:
        try:
            for iteration in itertools.count():
                # Max requests limit hit, shutdown. TODO - put logic in worker service (max_mem_usage, etc)
                if service.max_requests and iteration >= service.max_requests:
                    msg = 'Pool worker reached max request limit ({0})'.format(service.max_requests)
                    service.log.info(msg)
                    responses.put(Response(service.id, msg=msg))
                    break

                # Get next request within `service.max_request_timeout` seconds. If no new requests
                # are received, shutdown.
                try:
                    request = requests.get(block=True, timeout=service.max_request_timeout)
                except requests.empty_exception:
                    # No requests within timeout, kill ourselves.
                    msg = 'Pool worker received no requests after {0} seconds'.format(service.max_request_timeout)
                    service.log.info(msg)
                    responses.put(Response(service.id, msg=msg))
                    break

                # Got the poison pill, shutdown.
                if request is requests.poison_pill:
                    msg = 'Pool worker received shutdown request'
                    service.log.info(msg)
                    responses.put(Response(service.id, msg=msg))
                    break

                # Notify caller we're running their request.
                responses.put(Response(service.id, request.future_id, state=FutureState.Running))

                with AsyncResult() as result:
                    try:
                        result.result = request.func(*request.args, **request.kwargs)
                    except Exception:
                        result.exc_info = sys.exc_info()
                    except BaseException:
                        raise
                    finally:
                        responses.put(Response(service.id, request.future_id, result, FutureState.Completed))
        finally:
            requests.task_done()
    except Exception:
        msg = 'Pool worker encountered unhandled exception {0}'.format(traceback.format_exc())
        service.log.exception(msg)
        responses.put(Response(service.id, msg=msg))
    finally:
        service.log.info('Pool worker shutting down')



# async service gets a func, args, kwargs
# it creates a request object inside a runner and returns a future for it
# the request runner gets it
# marks it as queued or whatever state and removes any cancelled ones
# it then puts it into the proper worker pool
# the worker pool gets a func, args, kwargs
# the worker pool puts it into its worker task queue
# if its a MP worker pool, it has a greenlet worker pool inside of it (runners)
# this pool maps 1:1 with processes....

class PoolService(Service):
    """
    """

    __abstract__ = True

    #:
    #:
    queue_flush_timeout = ConfigAttribute(5)

    #:
    #:
    pool_min_size = None

    #:
    #:
    pool_max_size = None

    #:
    #:
    request_runner_class = ConfigAttribute()

    #:
    #:
    response_runner_class = ConfigAttribute()

    #:
    #:
    callback_runner_class = ConfigAttribute()

    #:
    #:
    pool_worker_class = ConfigAttribute()

    #:
    #:
    request_queue_class = ConfigAttribute()

    #:
    #:
    response_queue_class = ConfigAttribute()

    #:
    #:
    callback_queue_class = ConfigAttribute()

    #:
    #:
    futures_class = ConfigAttribute()

    #:
    #:
    requests = DependencyAttribute(config='REQUEST_QUEUE_CLASS')

    #:
    #:
    responses = DependencyAttribute(config='RESPONSE_QUEUE_CLASS')

    #:
    #:
    callbacks = DependencyAttribute(config='CALLBACK_QUEUE_CLASS')

    #:
    #:
    futures = DependencyAttribute(config='FUTURES_CLASS')

    @running
    def spawn(self, func, *args, **kwargs):
        """
        """
        future = self.futures.next()
        request = Request(future.id, func, args, kwargs, self.workers)
        self.requests.put(request)
        return future

    @running
    def spawn_later(self, seconds, func, *args, **kwargs):
        """
        """
        future = self.futures.next()
        request = Request(future.id, func, args, kwargs, self.workers, seconds)
        self.requests.put(request)
        return future

    @running
    def spawn_runner(self, func, *args, **kwargs):
        """
        """
        pass

    @running
    def spawn_runner_later(self, seconds, func, *args, **kwargs):
        """
        """
        pass

    def workers(self):
        """
        """
        return self.services.by_type(self.pool_worker_class)

    def grow(self):
        """
        """
        workers = self.services.by_type(self.pool_worker_class)
        size = len(workers)
        lwm = self.pool_min_size
        hwm = self.pool_max_size

        #if empty or (size < lwm or ):
        if size < lwm or (lwm <= size <= hwm and self.tasks.qsize() > 0):
            # spawn new fucker
            pass

#     def size(self):
#         return self.pool_max_size
#
#     def full(self):
#         return len(self) >= self.size()
#
#     def empty(self):
#         """
#         """
#         return len(self) == 0
#
#     def remove(self, service_id):
#         """
#         """
#         service = self.services.get(service_id)
#         if service is not None:
#             self.detach(service)

    def on_started(self, *args, **kwargs):
        """
        Callback fired when the service and all of its children are started.
        """
        # Start request runner which spawns new workers and populates the task queue.
        self.spawn_runner(self.request_runner, self.services, self.futures, self.requests, self.tasks, self.pool_worker_class)

        # Start response runner which processes completed jobs and populates futures.
        self.spawn_runner(self.response_runner, self.services, self.futures, self.responses, self.callbacks)

        # Start callback runner which processes future callbacks outside of the response processing loop.
        self.spawn_runner(self.callback_runner, self.futures, self.callbacks)

    def on_stopping(self, *args, **kwargs):
        """
        Callback fired when the service first begins to stop.
        """
        msg = 'Stopping with {0} requests, {1} responses and {2} callbacks remaining.'.format(self.requests.qsize(),
                                                                                              self.responses.qsize(),
                                                                                              self.callbacks.qsize())
        self.log.debug(msg)

        # Notify queues that the hammer is coming down and to finish up.
        for queue in (self.requests, self.responses, self.callbacks):
            queue.notify_stop()

        # Be nice and allow the queues some time to flush.
        # If they don't finish before the timeout, we continue on with stopping the service. In this
        # case, they'll still have a bit more time to flush as the workers which consume these
        # queues will provide some time to finish before a forceful stop.
        for queue in (self.requests, self.responses, self.callbacks):
            queue.join(self.queue_flush_timeout)

    def on_stopped(self, *args, **kwargs):
        """
        Callback fired when the service and all of its children have stopped.
        """
        timeout = self.queue_flush_timeout

        # Notify if we failed to process all requests before timeout.
        if not self.requests.empty():
            msg = 'Failed to complete processing of {0} requests after {1} seconds.'.format(self.requests.qsize(),
                                                                                            timeout)
            self.log.warning(msg)

        # Notify if we failed to process all responses before timeout.
        if not self.responses.empty():
            msg = 'Failed to complete processing of {0} responses after {1} seconds.'.format(self.responses.qsize(),
                                                                                             timeout)
            self.log.warning(msg)

        # Notify if we failed to process all callbacks before timeout.
        if not self.callbacks.empty():
            msg = 'Failed to complete processing of {0} callbacks after {1} seconds.'.format(self.callbacks.qsize(),
                                                                                             timeout)
            self.log.warning(msg)

    @staticmethod
    def request_runner(service, pool):
        """
        Function executed by runner pool which is responsible for processing incoming async requests,
        updating futures, and scheduling requests on the worker pool.
        """
        service.log.debug('Request runner starting')
        try:
            futures = pool.futures
            requests = pool.requests
            tasks = pool.tasks
            while True:
                try:
                    request = requests.get(block=True)

                    # Got the poison pill, begin shutdown process.
                    if request is requests.poison_pill:
                        if requests.empty():
                            service.log.debug('Request runner received shutdown command')
                            break
                        # Still have requests to process in the queue, put the poison pill back into the queue
                        # and attempt to finish the remaining requests.
                        requests.notify_stop()
                        continue

                    # Find the future for the request and confirm it's ready to run.
                    future = futures.get(request.future_id)
                    if future is None:
                        service.log.warning('Request contains unknown future id {0}.'.format(request.future_id))
                        continue

                    # If the future was already cancelled by the user, we should ignore it.
                    # if future.cancelled():
                    #     future.notify_cancel()
                    #     continue

                    pool.grow()

                    future.set_state(ExecutionState.Queued)
                    tasks.put(request.func, request.args, request.kwargs)
                except Exception as e:
                    service.log.info('FUCK  {0}'.format(str(e)))
                finally:
                    requests.task_done()
        except Exception:
            service.log.exception('Request runner raised unhandled exception')
        finally:
            service.log.info('Request runner shutting down')

    @staticmethod
    def response_runner(service, services, futures, responses, callbacks):
        """
        Function executed by runner pool which is responsible for processing incoming async responses
        from workers, updating futures and shuffling futures to the callback worker for processing.
        """
        service.log.info('Response runner starting')
        try:
            while True:
                try:
                    response = responses.get(block=True)
                    service.log.error('GOT RESPONSE!@')
                    service.log.error(response)

                    # Got the poison pill, begin shutdown process.
                    if response is responses.poison_pill:
                        if responses.empty():
                            service.log.info('Response runner received shutdown command')
                            break
                        # Still have responses to process in the queue, put the poison pill back into the queue
                        # and attempt to finish the remaining responses.
                        responses.notify_stop()
                        continue

                    # Sanity
                    if response is None:
                        service.log.warning('Response runner got an empty response!')
                        continue

                    # A worker response with no future id or state means it didn't process
                    # any tasks and shutdown.
                    future_id = response.future_id
                    if future_id is None:
                        worker = services.get(response.service_id)
                        if worker is None:
                            service.log.warning('Response contains unknown worker id {0}'.format(response.service_id))
                            continue
                        services.detach(worker)
                        service.log.info('Worker {0} shutdown: {1}.'.format(response.service_id, response.msg))

                    # Grab future tied to this response so we can notify caller.
                    future = futures.get(response.future_id)
                    if future is None:
                        service.log.warning('Response contains unknown future id {0}.'.format(response.future_id))
                        continue

                    # If the future has callbacks, pass it off to a different runner so we don't block
                    # the response worker if some caller happens to attach a blocking function to the future.
                    # If there aren't any callbacks, just mark the future from the response as it shouldn't block.
                    if future.has_callbacks():
                        callbacks.put(response)
                        continue

                    future.set_from_response(response)
                finally:
                    responses.task_done()
        except Exception:
            service.log.exception('Response runner raised unhandled exception')
        finally:
            service.log.info('Response runner shutting down')

    @staticmethod
    def callback_runner(service, futures, callbacks):
        """
        Function executed by runner pool which is responsible for processing incoming requests
        from the response runner and executing the future updates in a separate context so user callbacks
        are run outside of the request/response loop.
        """
        service.log.info('Callback runner starting')
        try:
            while True:
                try:
                    response = callbacks.get(block=True)

                    # Got the poison pill, begin shutdown process.
                    if response is callbacks.poison_pill:
                        if callbacks.empty():
                            service.log.info('Callback runner received shutdown command')
                            break
                        # Still have callbacks to process in the queue. Put the poison pill back into the queue
                        # and attempt to finish the remaining callbacks.
                        callbacks.notify_stop()
                        continue

                    # Grab future tied to this response so we can notify the caller
                    # and raises its callbacks.
                    future = futures.get(response.future_id)
                    if future is None:
                        service.log.warning('Callback contains unknown future id {0}.'.format(response.future_id))
                        continue

                    # Set the future result from the response which will fire the attached callbacks.
                    future.set_from_response(response)
                finally:
                    callbacks.task_done()
        except Exception:
            service.log.exception('Callback runner raised unhandled exception')
        finally:
            service.log.info('Callback runner shutting down')


class GreenletPoolService(Service):
    """
    """

    #:
    #:
    __abstract__ = True


# class WorkerContext(object):
#     """
#     """
#
#     __slots__ = ('pool_id', 'requests', 'responses')
#
#     def __init__(self, pool):
#         self.pool_id = pool.id
#         self.requests = pool.requests
#         self.responses = pool.responses


# class PoolService(Service):
#     """
#     """
#
#     #:
#     #:
#     __abstract__ = True
#
#     # #:
#     # #:
#     # pool_class = None
#     #
#     # #:
#     # #:
#     # pool = None
#     #
#     # #:
#     # #:
#     # pool_worker_context_class = ConfigAttribute()
#     #
#     # #:
#     # #:
#     # pool_max_size = ConfigAttribute()
#     #
#     # #:
#     # #:
#     # pool_worker_class = ConfigAttribute()
#     #
#     # #:
#     # #:
#     # pool_runner_class = ConfigAttribute()
#     #
#     # #:
#     # #:
#     # pool_worker_channel_class = ConfigAttribute()
#     #
#     # #:
#     # #:
#     # response_queue_class = ConfigAttribute(QueueService)
#     #
#     # #:
#     # #:
#     # request_queue_class = ConfigAttribute(QueueService)
#     #
#     # #:
#     # #:
#     # futures_class = ConfigAttribute(FutureService)
#     #
#     # #:
#     # #:
#     # worker_max_requests = ConfigAttribute()
#     #
#     # #:
#     # #:
#     # workers = DependencyAttribute(config='POOL_WORKER_CLASS')
#     #
#     # #:
#     # #:
#     # requests = DependencyAttribute(config='REQUEST_QUEUE_CLASS')
#     #
#     # #:
#     # #:
#     # responses = DependencyAttribute(config='RESPONSE_QUEUE_CLASS')
#     #
#     # #:
#     # #:
#     # futures = DependencyAttribute(config='FUTURES_CLASS')
#     #
#     # #:
#     # #:
#     # channel = DependencyAttribute(config='CHANNEL_CLASS')
#
#     # ...
#     # store parent queue
#
#
#     #:
#     #:
#     request_queue_class = ConfigAttribute(QueueService)
#
#     #:
#     #:
#     response_queue_class = ConfigAttribute(QueueService)
#
#     #:
#     #:
#     futures_class = ConfigAttribute(FutureService)
#
#     #:
#     #:
#     pool_worker_class = ConfigAttribute()
#
#     #:
#     #:
#     pool_worker_context_class = ConfigAttribute()
#
#     #:
#     #:
#     worker_max_requests = ConfigAttribute()
#
#     #:
#     #:
#     requests = DependencyAttribute(config='REQUEST_QUEUE_CLASS')
#
#     #:
#     #:
#     responses = DependencyAttribute(config='RESPONSE_QUEUE_CLASS')
#
#     #:
#     #:
#     channel = DependencyAttribute(config='CHANNEL_CLASS')
#
#     #:
#     #:
#     futures = DependencyAttribute(config='FUTURES_CLASS')
#
#     def on_started(self, *args, **kwargs):
#         """
#         """
#
#
#     @running
#     def spawn(self, func, *args, **kwargs):
#         """
#         """
#         future = self.futures.next()
#         request = Request(future.id, func, args, kwargs)
#         self.requests.put(request)
#         return future
#
#         # worker = self.spawn_pool_worker(func)
#         # worker.start(func, *args, **kwargs)
#         # return worker
#
#         #ctx = WorkerContext(self)
#         #config = self.config.section('worker')
#         #worker = self.child(self.pool_worker_class, name=resolve_callable(func),)
#         #worker.start(func, *args, **kwargs)
#         #worker.spawn_pool(func, *args, **kwargs)
#         #return worker
#
#     @running
#     def spawn_later(self, seconds, func, *args, **kwargs):
#         """
#         """
#         future = self.futures.next()
#         request = Request(future.id, func, args, kwargs, seconds)
#         self.requests.put(request)
#         return future
#
#         # worker = self.spawn_pool_worker(func)
#         # worker.start_later(seconds, func, *args, **kwargs)
#         # return worker
#
#         #worker = self.child(self.pool_worker_class, name=resolve_callable(func))
#         #worker.start_later(seconds, func, *args, **kwargs)
#         #worker.spawn_pool_later(seconds, func, *args, **kwargs)
#         #return worker
#
#     @running
#     def spawn_pool_worker(self):
#         """
#         called from async service when counts are off and need a other worker
#         """
#         config = self.config.section('worker')
#         context = self.pool_worker_context_class(self)
#         name = '[{0}] Worker {1}'.format(self.name, len(self.services))
#         worker = self.child(self.pool_worker_class, name=name, config=config, context=context)
#         worker.start()
#         return worker
#
#     @staticmethod
#     def pool_runner():
#         """
#         """
#         #
#         pass

    #
    # @running
    # def spawn_job(self, job):
    #     """
    #     """
    #     return self.spawn(job.func, *job.args, **job.kwargs)
    #
    # @running
    # def spawn_job_later(self, seconds, job):
    #     """
    #     """
    #     return self.spawn_later(seconds, job.func, *job.args, **job.kwargs)

    # @staticmethod
    # def request_runner(service, futures, workers, requests):
    #     pass
    #
    # @staticmethod
    # def response_runner():
    #     pass
    #
    # @staticmethod
    # def callback_runner():
    #     pass
    #
    #
    # @staticmethod
    # def pool_runner(service, workers, futures, requests, responses, callbacks):
    #     """
    #     """
    #     def on_request():
    #         pass
    #
    #     def on_response():
    #         pass
    #
    #     def on_callback():
    #         pass
    #
    #     try:
    #         while True:
    #             try:
    #                 channel, item = channel.select(block=True)
    #
    #                 # Got the poison pill, begin shutdown process.
    #                 if item is channel.poison_pill:
    #                     if channel.empty():
    #                         service.log.info('Pool runner received shutdown command')
    #                         break
    #                     # Still have requests/responses to process in the queue. Put the pill
    #                     # back into the channel (at the end) and continue processing.
    #                     channel.notify_stop()
    #                     continue
    #
    #                 # Got a request,
    #                 if channel is requests:
    #                     # Got the poison pill, begin shutdown process.
    #                     if item is channel.poison_pill:
    #                         service.log.info('')
    #                         pass
    #
    #                 # Got a response,
    #                 if channel is responses:
    #                     # ...
    #                     if item is channel.poison_pill:
    #                         pass
    #
    #                     if item is None:
    #                         pass
    #
    #                     future = futures.get(item.future_id)
    #                     if future is None:
    #                         pass
    #
    #                     if future.has_callbacks():
    #                         callbacks.put(item)
    #                         continue
    #
    #                     future.set_from_response(item)
    #
    #                     # ...
    #                     # process worker shutdown response
    #
    #                 # Got a callback
    #                 if channel is callbacks:
    #                     pass
    #
    #             finally:
    #                 channel.task_done()
    #     except Exception:
    #         pass
    #     finally:
    #         pass
    #     # try:
    #     #     while True:
    #     #         try:
    #     #             result = results.get(block=True)
    #     #
    #     #             if result is None:
    #     #                 service.log.warning('Pool runner got a None result?!?!?!?')
    #     #                 continue
    #     #
    #     #             # Got the poison pill, time to shutdown.
    #     #             if result is results.poison_pill:
    #     #                 if results.empty():
    #     #                     service.log.info('Pool runner received shutdown command')
    #     #                     break
    #     #                 # Still have results to process in the queue, put the poison pill back
    #     #                 # into the queue and attempt to finish remainder.
    #     #                 results.notify_stop()
    #     #                 continue
    #     #
    #     #             # If this pool has a queue from the service making the requests
    #     #             # shuffle the results upstream to be processed and move on.
    #     #             if requests is not None:
    #     #                 results.put(result)
    #     #                 continue
    #     #
    #     #             # ...
    #     #
    #     #         finally:
    #     #             results.task_done()
    #     # except Exception:
    #     #     pass
    #     # finally:
    #     #     pass
    #
    #
    # #
    # # @running
    # # def spawn(self, job):
    # #     """
    # #     """
    # #     worker = self.child(self.pool_worker_class, name=job.name)
    # #     worker.spawn(job)
    # #     return worker
    # #
    # # @running
    # # def spawn_later(self, seconds, job):
    # #     """
    # #     """
    # #     worker = self.child(self.pool_worker_class, name=job.name)
    # #     worker.spawn_later(seconds, job)
    # #     return worker
    #
    # def spawn_runner(self):
    #     pass



# thoughts:

# tie queue class to pool service (specific to exedcution type)
# tie future class in here? pass it down to the worker who sets that data in self.call when completed
# spawn returns a future class which the async service gets a reference to deal with
# when the worker finishes, it sets the values on the future and detaches from the parent (worker ctx)
# the pool service extends the detach method and scoops the future out of the dying worker (still in worker ctx)


# or...
# pool has the queue service
# which it passes down to its workers who put values in there
# the pool exposes this queue to the async service
# the async service creates a channel over all execution typed queues and consumes the responses