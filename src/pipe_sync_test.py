from multiprocessing import Pipe, Process, Lock
from array_queue import ArrayQueue
import ctypes, numpy, os, time
from numpy.ctypeslib import as_ctypes


def operation(Q):
    data = as_ctypes(numpy.zeros(512, 'd'))
    pid = os.getpid()
    for i in xrange(3):
        Q.put(data)
    
    for i in xrange(10000):
        Q.put(data)
        j = Q.get()
        print len(j), pid
    return True

if __name__=="__main__":
    Q = ArrayQueue(ctypes.c_double*512, 20)
    
    p1 = Process(target=operation, args=(Q,))
    p2 = Process(target=operation, args=(Q,))
    
    p1.start()
    p2.start()
    
    p1.join()
    p2.join()
     