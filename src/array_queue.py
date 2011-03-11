import weakref
import numpy
from multiprocessing.sharedctypes import RawArray
from multiprocessing import Pipe, Lock
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
        stock_out, stock_in = Pipe(duplex=False)
        queue_out, queue_in = Pipe(duplex=False)
        
        stock_out_lock = Lock()
        stock_in_lock = Lock()
        queue_out_lock = Lock()
        queue_in_lock = Lock()
        
        for i in xrange(size):
            stock_in.send(i)
        self.map={}
        
        self._put_obj = (stock_out, stock_out_lock , queue_in, queue_in_lock)
        self._ret_obj = (stock_in, stock_in_lock)
        self._get_obj = (queue_out, queue_out_lock)
            
        
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
        stock_out,stock_out_lock , queue_in, queue_in_lock = self._put_obj
        
        with stock_out_lock:
            if block:
                if timeout is None:
                    idx = stock_out.recv()
                else:
                    if stock_out.poll(timeout):
                        idx = stock_out.recv()
                    else:
                        raise Full
            else:
                if stock_out.poll():
                    idx = stock_out.recv()
                else:
                    raise Full
        
        self.buffer[idx] = scalar
        #print "sending", idx
        with queue_in_lock:
            queue_in.send(idx)
        
    def _finalise(self, r):
        idx,R = self.map.pop(id(r))
        #print "returning", idx
        stock_in, lock = self._ret_obj
        with lock:
            stock_in.send(idx)
        
    def close(self):
        put_obj = self._put_obj
        with put_obj[3]:
            put_obj[2].send(None)
        
    def get(self, block=True, timeout=None):
        queue_out, lock = self._get_obj
        
        with lock:
            if block:
                if timeout is None:
                    idx = queue_out.recv()
                else:
                    if queue_out.poll(timeout):
                        idx = queue_out.recv()
                    else:
                        raise Empty
            else:
                if queue_out.poll():
                    idx = queue_out.recv()
                else:
                    raise Empty
            
        if idx is None:
            raise EOFError
        value = self.buffer[idx]
        #print "ID", id(value), sys.getrefcount(value)
        r = weakref.ref(value, self._finalise)
        self.map[id(r)]=(idx, r)
        return value