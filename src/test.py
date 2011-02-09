from array_pipe import ArrayPipe
import ctypes
import numpy
import time
import sys

from multiprocessing import Process, Pipe


datasize = 2048*4


def operation(conn):
    try:
        while True:
            data = conn.recv()
            #a = data[0]+1
            #del data
    except EOFError:
        return
        

def test1():
    sender, receiver = ArrayPipe(ctypes.c_double*datasize, 20)
    
    p = Process(target=operation, args=(receiver,))
    p.start()
    data = numpy.linspace(0,1,datasize)
    try:
        for i in xrange(10000):
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
        for i in xrange(10000):
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
    print "total3", ct
    
def test3():
    receiver, sender = Pipe(False)
    
    p = Process(target=operation3, args=(receiver,))
    p.start()
    data = numpy.linspace(0,1,datasize)
    try:
        for i in xrange(10000):
            sender.send_bytes(data)
    finally:
        sender.send("\z")
        sender.close()    
        receiver.close()
        p.join()

if __name__=="__main__":
    print "start"
    start = time.time()
    test1()
    print "test1 took",time.time() - start
    start = time.time()
    test3()
    print "test3 took",time.time() - start
    start = time.time()
    test2()
    print "test2 took",time.time() - start