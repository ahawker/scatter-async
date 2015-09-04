"""
    scatter_async.channel
    ~~~~~~~~~~~~~~~~~~~~~

"""
__all__ = ('ScatterChannel', 'Channel', 'ChannelService')


import contextlib
import itertools

from scatter.config import ConfigAttribute
from scatter.descriptors import cached
from scatter.service import Service


class ScatterChannel(object):

    def __init__(self):
        self.queue = None
        self.channel_counter = itertools.count()



# MP channel:
#


class Channel(object):

    #:
    queue_cls = None

    def __init__(self, *args, **kwargs):
        self._queue = self.queue_cls(*args, **kwargs)
        self._queues = []

    def get(self):
        pass

    def get_nowait(self):
        pass

    def put(self):
        pass

    def put_nowait(self):
        pass

    def join(self):
        pass

    def qsize(self):
        pass

    def full(self):
        pass

    def empty(self):
        pass

    def task_done(self):
        pass


# channel service has child services
# each child service is a channel
# when you call get on the channel service, you get from its single, internal queue
# when you call put on the channel service, you



#def runner(channel):
# req = channel.get()
# ...
# res = do_shit()
# channel.put(res)





class ChannelService(Service):
    """

    """

    __abstract__ = True

    #:
    #:
    poison_pill = object()

    #:
    #:
    channel_class = ConfigAttribute(Channel)

    #:
    #:
    channel_max_size = ConfigAttribute()

    @cached
    def channel(self):
        return self.channel_class(self.channel_max_size)

    def __init__(self):
        self.channels = []

    def on_initializing(self, *args, **kwargs):
        """
        """
        self.channel = None

    def add_queue(self, queue):
        pass

    def get(self, block=True, timeout=None):
        return self.channel.get(block, timeout)

    def put(self, channel, item, block=True, timeout=None):
        return self.channel.put((channel, item), block, timeout)

    @contextlib.contextmanager
    def select(self):
        pass


# have libs extend the channel service
# the channel service will wrap a priority queue implementation
