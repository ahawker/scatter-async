"""
    tests.test_service
    ~~~~~~~~~~~~~~~~~~

    Contains tests for the :module: `scatter_async.service` module.
"""





#import pytest

#from scatter_async.service import AsyncService
#from scatter.exceptions import ScatterExit

#from scatter_gevent import *

# @pytest.fixture(scope='module')
# def async_service_cls():
#     return AsyncService
#
#
# def test_thing(async_service_cls):
#     with async_service_cls.new() as s:
#         print s

#from gevent import monkey
#monkey.patch_all()

#import gipc

#from scatter_multiprocessing import *
from scatter_threading import *
#from scatter_multiprocessing import *


count = 0


def thing(service):
    """
    """
#    try:
        #try:
    for _ in xrange(10000000):
        #if _ % 100000 == 0:
            #service.log.info('.')
        #    gevent.sleep(0)
        continue

#    except Exception as e:
#        service.log.error(locals())
#        service.log.error('FUUuUUUCIKKDKFKDFKDFKDFKD')
#        return

    service.log.info('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~WORKER DONE!')
    global count
    count += 1
    return None
            #service.log.info(_)
    #except ScatterExit:
    #    service.log.warning('FUC<KING ERXITEXIIODFSJLKFDKLF(*)()()()()()()()()()()')

import time
import threading



def main():
    global count
    with ThreadAsyncService.new() as s:
    #    s = ThreadAsyncService.new()
        s.log.info('AsyncService is RUNNING!')
        for _ in xrange(1):
            s.spawn(thing)


        #s.log.info(future.id)
        #s.log.info(future.state)
        #import time
        #for _ in range(60):
        #    time.sleep(1)
        #time.sleep(60)


        # def wait(service):
        #     service.log.info('SERVICE IN WAIT STATE')
        #     service.join()
        #     service.log.info('SERVICE EXITING WAIT STATE')
        #
        # t = threading.Thread(target=wait, args=(s,))
        # t.start()
        #


        while True:
            #time.sleep(1)
            try:
                time.sleep(1)
                s.log.info('WORKERS DONE {}'.format(count))
            except KeyboardInterrupt:
                s.log.info('keyboard int')
                s.stop()
                s.log.info('stp')
                x=s.join()
                s.log.info('joined {}'.format(x))
                if x is True:
                    break


        # i = 0
        # while True:
        #     if i >= 2:
        #         #print "STOPOPOPOOPPOOFDPFPODPFOPDOFPODFDF"
        #         s.log.error('ABOUT TO CALL STOP')
        #         s.stop()
        #         s.log.error('STOP CALL RETURNED!')
        #         t.join()
        #         s.log.error('doene!!')
        #         #print 'JOINING!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
        #         #x=s.join(1)
        #         #s.log.error('JOIN RETURNED!!! {}'.format(x))
        #         #print '*****************************************'
        #         break
        #     time.sleep(1)
        #     i += 1

        #s.log.info(future)
        #global count
        print count


#
# def main2():
#     import sys
#     print sys.modules.keys()
#     print sys.modules.get('scatter_async')
#     print sys.modules.get('scatter.ext.async')
#     import pkg_resources
#     print sys.modules.get('scatter_async')
#     print sys.modules.get('scatter.ext.async')
#     print pkg_resources.working_set.entries
#
#     #print pkg_resources.working_set.entries
#     for dist in pkg_resources.working_set:
#         if not dist.project_name.startswith('scatter'):
#             continue
#         print dist
#         #print type(dist)
#         print 'check'
#         print sys.modules.get('scatter_async')
#         print sys.modules.get('scatter.ext.async')
#         for ep in dist.get_entry_map(None).values():
#             #xxx = dist.get_entry_info(None, ep)
#             #print xxx
#             #print type(xxx)
#             print 'hello'
#             print ep
#             print type(ep)
#             print type(ep['enabled'])
#             xxx=ep['enabled']
#             print dir(xxx)
#             print xxx.dist.project_name
#             return
#             print ep['enabled']
#             print type(ep)
#             m = ep['enabled'].load()
#             print m
#             #p = ep['package'].load()
#             print ep['package']
#             import sys
#             print sys.modules.get('scatter_async')
#             print 'ext'
#             print sys.modules.get('scatter.ext.async')
#             print id(sys.modules.get('scatter_async'))
#             print id(sys.modules.get('scatter.ext.async'))
#             print sys.modules.get('scatter_gevent')
#             print sys.modules.get('scatter.ext.gevent')
#
#
#             return
#
#         #print dist.egg_name()
#         #print type(dist)
#         # for x,v in dist.get_entry_map(None).iteritems():
#         #     print 'entry'
#         #     print x
#         #     print type(x)
#         #     print v
#         #     print type(v)
#     print '~~~~~~'
#
#     # xxx= list(pkg_resources.iter_entry_points(None))
#     # for i in xxx:
#     #     print i
#     # #print xxx
#     # print len(xxx)


if __name__ == '__main__':
    main()