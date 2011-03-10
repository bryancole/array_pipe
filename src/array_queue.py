import weakref, sys, gc
import numpy
from numpy.ctypeslib import as_ctypes, as_array
from multiprocessing.sharedctypes import RawArray
from multiprocessing import Pipe
from Queue import Empty, Full


class ArrayQueue(object):
    """
    Multiprocess queue using shared memory
    """
    def __init__(self, struct, size=20):
        """struct - a ctypes object or numpy dtype
        size - number of slots in the buffer
        """
        buf = RawArray(struct, int(size))
        self._buffer = buf
        self.buffer = numpy.frombuffer(buf, dtype=numpy.dtype(buf._type_))
        self.stock_out, self.stock_in = Pipe(duplex=False)
        self.queue_out, self.queue_in = Pipe(duplex=False)
        for i in xrange(size):
            self.stock_in.send(i)
        self.map={}
        
    def __getstate__(self):
        d = super(ArrayQueue, self).__getstate__()
        d.pop("_map", None)
        d.pop("buffer", None)
        return d
    
    def __setstate__(self, d):
        super(ArrayQueue,self).__setstate__(d)
        d['map']={}
        buffer = self._buffer
        dt = buffer._type_
        d['buffer']=numpy.frombuffer(buffer, 
                                     dtype=numpy.dtype(dt))
        
    def put(self, scalar, block=True, timeout=None):
        recv = self.stock_out.recv
        poll = self.stock_out.poll
        
        if block:
            if timeout is None:
                idx = recv()
            else:
                if poll(timeout):
                    idx = recv()
                else:
                    raise Full
        else:
            if poll():
                idx = recv()
            else:
                raise Full
        
        self.buffer[idx] = scalar
        #print "sending", idx
        self.queue_in.send(idx)
        
    def _finalise(self, r):
        idx,R = self.map.pop(id(r))
        #print "returning", idx
        self.stock_in.send(idx)
        
    def close(self):
        self.queue_in.send(None)
        
    def get(self, block=True, timeout=None):
        recv = self.queue_out.recv
        poll = self.queue_out.poll
        if block:
            if timeout is None:
                idx = recv()
            else:
                if poll(timeout):
                    idx = recv()
                else:
                    raise Empty
        else:
            if poll():
                idx = recv()
            else:
                raise Empty
            
        if idx is None:
            raise EOFError
        value = self.buffer[idx]
        #print "ID", id(value), sys.getrefcount(value)
        r = weakref.ref(value, self._finalise)
        self.map[id(r)]=(idx, r)
        #print "sending B", idx
        #p.send(idx)
        return value