import weakref
from multiprocessing.sharedctypes import RawArray
from multiprocessing import Pipe, Lock, RawValue, Condition
from Queue import Empty, Full


class ArrayQueue2(object):
    """Multiprocess queue using shared memory and no pipes
    """
    def __init__(self, struct, size=20):
        size = int(size)
        self.buffer = RawArray(struct, size)
        self.stock = RawArray('I', size)
        self.queue = RawArray('I', size)
        
        self.stock[:] = xrange(size)
        
        self.stock_write = RawValue('I',0)
        self.stock_read = RawValue('I',0)
        self.queue_write = RawValue('I',0)
        self.queue_read = RawValue('I',0)
        
        self.put_cond = Condition(Lock())
        self.get_cond = Condition(Lock())
        
    def put(self, scalar):
        with self.put_cond:
            if self.stock_read.value == self.stock_write.value:
                self.put_cond.wait()


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
        with queue_in_lock:
            queue_in.send(None)
        with stock_in_lock:
            stock_in.send(None)
        
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
    