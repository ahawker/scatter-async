"""
    scatter-async
    ~~~~~~~~~~~~~

    Package which contains base constructs to be implemented by extensions for
    asynchronous features.

    :copyright: (c) 2014 Andrew Hawker.
    :license: ?, See LICENSE file.
"""

ENABLED = True

import itertools

from scatter_async import channel
from scatter_async.channel import *
from scatter_async import core
from scatter_async.core import *
from scatter_async import future
from scatter_async.future import *
from scatter_async import pool
from scatter_async.pool import *
from scatter_async import queue
from scatter_async.queue import *
from scatter_async import service
from scatter_async.service import *
from scatter_async import worker
from scatter_async.worker import *

__all__ = list(itertools.chain(channel.__all__,
                               core.__all__,
                               future.__all__,
                               pool.__all__,
                               queue.__all__,
                               service.__all__,
                               worker.__all__))



# ---------------------------------------------------------------------------------------------
# from .core import *
#
# # Event = None
# # Condition = None
# # Lock = None
# # RLock = None
# # Semaphore = None
# # BoundedSemaphore = None
# # Pool = None
# # Group = None
#
#
# #Multiprocessing (stdlib)
# try:
#     from .multiprocessing import ProcessService, Event, Condition, \
#         Lock, RLock, Semaphore, BoundedSemaphore, Queue, Signal
# except ImportError as e:
#     ProcessAsync = None
# else:
#     ProcessAsync = ProcessService
#     Event = Event
#     Condition = Condition
#     Lock = Lock
#     RLock = RLock
#     Semaphore = Semaphore
#     BoundedSemaphore = BoundedSemaphore
#     Queue = Queue
#     Signal = Signal
#
#
# #Threading (stdlib)
# try:
#     from .threading import ThreadingService, Event, Condition, \
#         Lock, RLock, Semaphore, BoundedSemaphore, Queue, Signal
# except ImportError:
#     ThreadingAsync = None
# else:
#     ThreadingAsync = ThreadingService
#     Event = Event
#     Condition = Condition
#     Lock = Lock
#     RLock = RLock
#     Semaphore = Semaphore
#     BoundedSemaphore = BoundedSemaphore
#     Queue = Queue
#     Signal = Signal
#
#
# #Eventlet
# try:
#     from .eventlet import EventletService, Event, \
#         Lock, RLock, Semaphore, BoundedSemaphore, Queue, Signal
# except ImportError:
#     EventletAsync = None
# else:
#     EventletAsync = EventletService
#     # Event = Event
#     # Condition = Condition
#     # Lock = Lock
#     # RLock = RLock
#     # Semaphore = Semaphore
#     # BoundedSemaphore = BoundedSemaphore
#     # Queue = Queue
#     # Signal = Signal
#
#
# #Gevent
# try:
#     from .gevent import GeventService, Event, \
#         Lock, RLock, Semaphore, BoundedSemaphore, Queue, Signal
# except ImportError:
#     GeventAsync = None
# else:
#     GeventAsync = GeventService
#     Event = Event
#     #Condition = Condition
#     Lock = Lock
#     RLock = RLock
#     Semaphore = Semaphore
#     BoundedSemaphore = BoundedSemaphore
#     Queue = Queue
#     Signal = Signal
