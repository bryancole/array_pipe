from multiprocessing import Pipe, Process, Lock
from array_queue import ArrayQueue
import ctypes, numpy, os, time


def operation(Q):
    data = numpy.zeros(512, 'd')
    pid = os.getpid()
    for i in xrange(3):
        Q.put(data)
    
    for i in xrange(10000):
        #with slock:
        Q.put(data)
        #with rlock:
        #time.sleep(0.1)
        j = Q.get()
        print j.shape, pid
    return True

if __name__=="__main__":
    Q = ArrayQueue(ctypes.c_double*512, 20)
    
    p1 = Process(target=operation, args=(Q,))
    p2 = Process(target=operation, args=(Q,))
    
    p1.start()
    p2.start()
    
    p1.join()
    p2.join()
     