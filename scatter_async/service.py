"""
    scatter_async.service
    ~~~~~~~~~~~~~~~~~~~~~
"""
__all__ = ('AsyncService',)


from scatter.config import ConfigAttribute
from scatter.service import Service, DependencyAttribute, running
from scatter_async.channel import ChannelService
from scatter_async.core import Request, Job, ExecutionState
from scatter_async.future import FutureService, FutureProxy
from scatter_async.pool import PoolService, GreenletPoolService
from scatter_async.queue import QueueService


class AsyncService(Service):
    """

    """

    #:
    #:
    __abstract__ = True

    #:
    #:
    queue_flush_timeout = ConfigAttribute(5)

    #:
    #:
    request_queue_class = ConfigAttribute(QueueService)

    #:
    #:
    response_queue_class = ConfigAttribute(QueueService)

    #:
    #:
    callback_queue_class = ConfigAttribute(QueueService)

    #:
    #:
    greenlet_pool_class = ConfigAttribute(GreenletPoolService)

    #:
    #:
    thread_pool_class = ConfigAttribute(PoolService)

    #:
    #:
    multiprocess_pool_class = ConfigAttribute(PoolService)

    #:
    #:
    runner_pool_class = ConfigAttribute(PoolService)

    #:
    #:
    futures_class = ConfigAttribute(FutureService)

    # #:
    # #:
    # channel_class = ConfigAttribute(ChannelService)

    #:
    #:
    greenlets = DependencyAttribute(config='GREENLET_POOL_CLASS')

    #:
    #:
    runners = DependencyAttribute(config='RUNNER_POOL_CLASS')

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

    # #:
    # #:
    # responses_channel = DependencyAttribute(config='CHANNEL_CLASS')

    # ...
    # find all registered worker pool types
    # channel = register all queues for all worker pools created
    # response runner will consume the channel
    # worker pools will produce results and PUT into their re

    @running
    def spawn(self, func, *args, **kwargs):
        """
        """
        return self.spawn_greenlet(func, *args, **kwargs)

    @running
    def spawn_later(self, seconds, func, *args, **kwargs):
        """
        """
        return self.spawn_greenlet_later(seconds, func, *args, **kwargs)

    @running
    def spawn_runner(self, func, *args, **kwargs):
        """
        """
        return self.spawn_greenlet_runner(func, *args, **kwargs)

    @running
    def spawn_runner_later(self, seconds, func, *args, **kwargs):
        """
        """
        return self.spawn_greenlet_runner_later(seconds, func, *args, **kwargs)

    @running
    def spawn_greenlet(self, func, *args, **kwargs):
        """
        """
        return self._spawn(self.greenlets, func, *args, **kwargs)

    @running
    def spawn_greenlet_runner(self, func, *args, **kwargs):
        """
        """
        return self._spawn_runner(self.greenlets, func, *args, **kwargs)

    @running
    def spawn_greenlet_later(self, seconds, func, *args, **kwargs):
        """
        """
        return self._spawn_later(self.greenlets, seconds, func, *args, **kwargs)

    @running
    def spawn_greenlet_runner_later(self, seconds, func, *args, **kwargs):
        """
        """
        return self._spawn_runner_later(self.greenlets, seconds, func, *args, **kwargs)

    @running
    def spawn_thread(self, func, *args, **kwargs):
        """
        """
        return self._spawn(self.threads, func, *args, **kwargs)

    @running
    def spawn_thread_runner(self, func, *args, **kwargs):
        """
        """
        return self._spawn_runner(self.threads, func, *args, **kwargs)

    @running
    def spawn_thread_later(self, seconds, func, *args, **kwargs):
        """
        """
        return self._spawn_later(self.threads, seconds, func, *args, **kwargs)

    @running
    def spawn_thread_runner_later(self, seconds, func, *args, **kwargs):
        """
        """
        return self._spawn_runner_later(self.threads, seconds, func, *args, **kwargs)

    @running
    def spawn_multiprocess(self, func, *args, **kwargs):
        """
        """
        return self._spawn(self.multiprocesses, func, *args, **kwargs)

    @running
    def spawn_multiprocess_runner(self, func, *args, **kwargs):
        """
        """
        return self._spawn_runner(self.multiprocesses, func, *args, **kwargs)

    @running
    def spawn_multiprocess_later(self, seconds, func, *args, **kwargs):
        """
        """
        return self._spawn_later(self.multiprocesses, seconds, func, *args, **kwargs)

    @running
    def spawn_multiprocess_runner_later(self, seconds, func, *args, **kwargs):
        """
        """
        return self._spawn_runner_later(self.multiprocesses, seconds, func, *args, **kwargs)

    def _spawn(self, pool, func, *args, **kwargs):
        """
        """
        return pool.spawn(func, *args, **kwargs)
        #return FutureProxy(future)
        # future = self.futures.next()
        # request = Request(future.id, func, args, kwargs, pool)
        # self.requests.put(request)
        # return future

    def _spawn_later(self, pool, seconds, func, *args, **kwargs):
        """
        """
        return pool.spawn_later(seconds, func, *args, **kwargs)
        #return FutureProxy(future)
        # future = self.futures.next()
        # request = Request(future.id, func, args, kwargs, pool, seconds)
        # self.requests.put(request)
        # return future

    def _spawn_runner(self, pool, func, *args, **kwargs):
        """
        """
        return pool.spawn(func, *args, **kwargs)

    def _spawn_runner_later(self, pool, seconds, func, *args, **kwargs):
        """
        """
        return pool.spawn_later(seconds, func, *args,  **kwargs)

    # @running
    # def spawn_later(self, seconds, func, *args, **kwargs):
    #     """
    #     """
    #     future = self.futures.next()
    #     request = Request(future.id, func, args, kwargs, seconds)
    #     self.requests.put(request)
    #     return future
    #
    # @running
    # def spawn_runner(self, func, *args, **kwargs):
    #     """
    #     """
    #     future = self.futures.next()
    #     request = Request(future.id, func, args, kwargs)
    #     #job = Job(self, request, self.runners)
    #     #self.runners.spawn(job)
    #     self.requests.put(request)
    #     return future
    #
    # @running
    # def spawn_runner_later(self, seconds, func, *args, **kwargs):
    #     """
    #     """
    #     future = self.futures.next()
    #     request = Request(future.id, func, args, kwargs, seconds)
    #     #job = Job(self, request, self.runners)
    #     self.runners.spawn(job)
    #     return future

    def on_initialized(self, *args, **kwargs):
        """
        """
        pass

    def on_started(self, *args, **kwargs):
        """
        Callback fired when the service and all of its children are started.
        """
        # Start request runner which spawns new workers and populates the task queue.
        self.spawn_runner(self.request_runner, self.futures, self.requests)

        # Start response runner which processes completed jobs and populates futures.
        self.spawn_runner(self.response_runner, self.futures, self.responses, self.callbacks)

        # Start callback runner which processes future callbacks outside of the response processing loop.
        self.spawn_runner(self.callback_runner, self.futures, self.callbacks)

    def on_stopping(self, *args, **kwargs):
        """
        Callback fired when the service first begins to stop.
        """
        msg = 'Stopping with {0} requests, {1} responses and {2} callbacks remaining.'.format(self.requests.qsize(),
                                                                                              self.responses.qsize(),
                                                                                              self.callbacks.qsize())
        self.log.info(msg)

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
    def request_runner(service, futures, requests):
        """
        Function executed by runner pool which is responsible for processing incoming async requests,
        updating futures, and scheduling requests on the worker pool.
        """
        service.log.info('Request runner starting')
        try:
            while True:
                try:
                    request = requests.get(block=True)

                    # Got the poison pill, begin shutdown process.
                    if request is requests.poison_pill:
                        if requests.empty():
                            service.log.info('Request runner received shutdown command')
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

                    future.set_state(ExecutionState.Running)

                    # Pass work down to specific async pool to run. Each pool manages its own
                    # set of futures, so link the top-level one to be notified when its underlying
                    # work is complete.
                    if request.seconds is None:
                        worker_future = request.pool.spawn(request.func, request.args, request.kwargs)
                    else:
                        worker_future = request.pool.spawn_later(request.seconds, request.func, request.args, request.kwargs)

                    #future.link(worker_future)

                    #workers.spawn(Job(service, request, workers))
                    #import time
                    #time.sleep(0.0001)
                except Exception as e:
                    service.log.info('FUCK  {0}'.format(str(e)))
                finally:
                    requests.task_done()
        except Exception:
            service.log.exception('Request runner raised unhandled exception')
        finally:
            service.log.info('Request runner shutting down')

    @staticmethod
    def response_runner(service, futures, responses, callbacks):
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

                    # # Cleanup worker service which hosted job execution as these are "one and done".
                    # worker = workers.get(response.service_id)
                    # if worker is None:
                    #     service.log.warning('Response contains unknown service id {0}'.format(response.service_id))
                    #     continue
                    # workers.detach(worker)
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



# async service has two queues for req/res for ALL types
# pool service has two queues for req/res for specific types
# pool worker uses pool service queues to process work
# process pool worker creates its own queue pair and a runnner to consume that shit
