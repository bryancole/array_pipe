from array_pipe import ArrayPipe
from array_queue import ArrayQueue
import ctypes
import numpy
import time
import sys

from multiprocessing import Process, Pipe
from numpy.ctypeslib import as_ctypes, as_array


datasize = 2048
reps = 100000


def operation(conn):
    try:
        while True:
            data = conn.recv()
    except EOFError:
        return

def test1():
    sender, receiver = ArrayPipe(ctypes.c_double*datasize, 20)
    
    p = Process(target=operation, args=(receiver,))
    p.start()
    data = numpy.linspace(0,1,datasize)
    try:
        for i in xrange(reps):
            sender.send(data)
    finally:
        sender.close()    
        p.join()
        
        
def operation2(conn):
    try:
        while True:
            data = conn.recv()
            if data is None:
                break
            #print data[:5]
            #a = data[0]+1
    except EOFError:
        return

def test2():
    receiver, sender = Pipe(False)
    p = Process(target=operation2, args=(receiver,))
    p.start()
    data = numpy.linspace(0,1,datasize)
    try:
        for i in xrange(reps):
            sender.send(data)
    finally:
        sender.send(None)
        sender.close()    
        p.join()
        
        
def operation3(conn):
    try:
        ct = 0
        buf = numpy.zeros(shape=(datasize,), dtype=numpy.double)
        size = buf.nbytes
        while True:
            n = conn.recv_bytes_into(buf)
            if n < size:
                break
            #a = buf[0] + 1
            ct += 1
    except EOFError:
        pass
    
def test3():
    receiver, sender = Pipe(False)
    
    p = Process(target=operation3, args=(receiver,))
    p.start()
    data = numpy.linspace(0,1,datasize)
    try:
        for i in xrange(reps):
            sender.send_bytes(data)
    finally:
        sender.send("\z")
        sender.close()    
        receiver.close()
        p.join()
        

def operation4(Q):
    try:
        while True:
            data = Q.get()
    except EOFError:
        return
    
def test4():
    ct = ctypes.c_double*datasize
    Q = ArrayQueue(ct, 20)
    p = Process(target=operation4, args=(Q,))
    p.start()
    data = ct()
    as_array(data)[:] = numpy.linspace(0,1,datasize)
    try:
        for i in xrange(reps):
            Q.put(data)
    finally:
        Q.close()    
        p.join()
    

if __name__=="__main__":
    print "start"
    start = time.time()
    test1()
    print "test1  (ArrayPipe) took",time.time() - start
    
    time.sleep(1)
    
    start = time.time()
    test4()
    print "test4 (ArrayQueue) took",time.time() - start
    
    time.sleep(1)
    
    start = time.time()
    test3()
    print "test3 (Pipe with buffer) took",time.time() - start
    
    time.sleep(1)
    
    start = time.time()
    test2()
    print "test2 (Pipe with pickle) took",time.time() - start
