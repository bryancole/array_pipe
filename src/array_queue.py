import weakref
from multiprocessing.sharedctypes import RawArray
from multiprocessing import Pipe, Lock, RawValue, Condition, Array
from Queue import Empty, Full
import numpy


class ArrayQueue2(object):
    """Multiprocess queue using shared memory and no pipes.
    
    Turns out to be slower than using a pair of Pipes
    """
    def __init__(self, struct, size=20):
        size = int(size)+1
        self._size = size
        self.buffer = Array(struct, size)
        #dt = numpy.dtype(struct)
        #self.buffer = numpy.frombuffer(self._buffer, dtype=dt) 
        self.stock = RawArray('I', size)
        self.queue = RawArray('I', size)
        
        self.stock_write = RawValue('I',0)
        self.stock_read = RawValue('I',0)
        self.queue_write = RawValue('I',0)
        self.queue_read = RawValue('I',0)
        
        self.stock[:] = xrange(size)
        self.stock_write.value = size-1
        
        self.stock_lock = Condition(Lock())
        self.queue_lock = Condition(Lock())
        
        self.map = {}
        
    def __getstate__(self):
        d = self.__dict__.copy()
        d.pop("map", None)
        return d
    
    def __setstate__(self, d):
        d['map']={}
        self.__dict__.update(d)
        
    def close(self):
        stock_lock = self.stock_lock
        size = self._size
        with stock_lock:
            stock_write = self.stock_write.value
            stock_read = self.stock_read.value
            while stock_read == (stock_write+1)%size:
                stock_lock.wait()
                stock_write = self.stock_write.value
                stock_read = self.stock_read.value
            #print "return", stock_read, stock_write
            self.stock[stock_read] = size+1
            stock_lock.notify()
        
        queue_lock = self.queue_lock
        with queue_lock:
            queue_write = self.queue_write.value
            queue_read = self.queue_read.value
            while queue_read == (queue_write+1)%size:
                queue_lock.wait()
                queue_write = self.queue_write.value
                queue_read = self.queue_read.value
            #print queue_read, queue_write
            self.queue[queue_write] = size+1
            self.queue_write.value = (queue_write + 1)%size
            queue_lock.notify()
        
    def put(self, scalar):
        size = self._size
        stock_lock = self.stock_lock
        with stock_lock:
            stock_write = self.stock_write.value
            stock_read = self.stock_read.value
            while stock_read == stock_write:
                #print "put wait", stock_read, stock_write
                stock_lock.wait()
                stock_write = self.stock_write.value
                stock_read = self.stock_read.value
            #print "put", stock_read, stock_write,
            idx = self.stock[stock_read]
            if idx == self._size+1: #it's been closed
                raise EOFError
            self.stock_read.value = (stock_read + 1)%size
            
        #print "put in ",
        self.buffer[idx] = scalar
        #print "buffer"
        
        queue_lock = self.queue_lock
        with queue_lock:
            queue_write = self.queue_write.value
            queue_read = self.queue_read.value
            while queue_read == (queue_write+1)%size:
                queue_lock.wait()
                queue_write = self.queue_write.value
                queue_read = self.queue_read.value
            #print queue_read, queue_write
            self.queue[queue_write] = idx
            self.queue_write.value = (queue_write + 1)%size
            queue_lock.notify()
            
    def _finalise(self, r):
        idx,R = self.map.pop(id(r))
        #print "returning", idx
        stock_lock = self.stock_lock
        size = self._size
        with stock_lock:
            stock_write = self.stock_write.value
            stock_read = self.stock_read.value
            while stock_read == (stock_write+1)%size:
                stock_lock.wait()
                stock_write = self.stock_write.value
                stock_read = self.stock_read.value
            #print "return", stock_read, stock_write
            self.stock[stock_write] = idx
            self.stock_write.value = (stock_write+1)%size
            stock_lock.notify()
            
    def get(self):
        queue_lock = self.queue_lock
        with queue_lock:
            queue_write = self.queue_write.value
            queue_read = self.queue_read.value
            while queue_read == queue_write:
                #print "get wait"
                queue_lock.wait()
                queue_write = self.queue_write.value
                queue_read = self.queue_read.value
            #print "get", queue_read, queue_write
            idx = self.queue[queue_read]
            if idx == self._size+1: #if it's been closed
                raise EOFError
            self.queue_read.value = (queue_read + 1)%self._size
        
        val = self.buffer[idx]
        
        r = weakref.ref(val, self._finalise)
        self.map[id(r)]=(idx, r)
        return val


class ArrayQueue(object):
    """
    Multiprocess queue using shared memory
    """
    def __init__(self, struct, size=20):
        """struct - a ctypes object or numpy dtype
        size - number of slots in the buffer
        """
        buf = RawArray(struct, int(size))
        self.buffer = buf
        #self.buffer = numpy.frombuffer(buf, dtype=numpy.dtype(buf._type_))
        stock_out, stock_in = Pipe(duplex=False)
        queue_out, queue_in = Pipe(duplex=False)
        
        stock_out_lock = Lock()
        stock_in_lock = Lock()
        queue_out_lock = Lock()
        queue_in_lock = Lock()
        
        self.stock_closed = RawValue('h', 0)
        self.queue_closed = RawValue('h', 0)
        
        for i in xrange(size):
            stock_in.send(i)
        self.map={}
        
        self._put_obj = (stock_out, stock_out_lock , queue_in, queue_in_lock)
        self._ret_obj = (stock_in, stock_in_lock)
        self._get_obj = (queue_out, queue_out_lock)
            
        
    def __getstate__(self):
        d = self.__dict__.copy()
        d.pop("map", None)
        return d
    
    def __setstate__(self, d):
        d['map']={}
        self.__dict__.update(d)
        
    def put(self, scalar, block=True, timeout=None):
        stock_out,stock_out_lock , queue_in, queue_in_lock = self._put_obj
        
        with stock_out_lock:
            if self.stock_closed.value:
                raise EOFError
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
                
            if idx is None:
                self.stock_closed.value = 1
                raise EOFError
        
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
        stock_out, stock_out_lock , queue_in, queue_in_lock = self._put_obj
        stock_in, stock_in_lock = self._ret_obj
        
        while not stock_out_lock.acquire(timeout=0.001):
            with stock_in_lock:
                stock_in.send(None)
        self.stock_closed.value = 1
        stock_out_lock.release()
                
        with queue_in_lock:
            queue_in.send(None)
        
        
    def get(self, block=True, timeout=None):        
        queue_out, lock = self._get_obj
        
        with lock:
            if self.queue_closed.value:
                raise EOFError
            
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
                self.queue_closed.value = 1
                raise EOFError
        
        value = self.buffer[idx]
        #print "ID", id(value), sys.getrefcount(value)
        r = weakref.ref(value, self._finalise)
        self.map[id(r)]=(idx, r)
        return value
    