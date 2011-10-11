import weakref, sys, gc
import numpy
from numpy.ctypeslib import as_ctypes, as_array
from multiprocessing.sharedctypes import RawArray
from multiprocessing import Pipe


def ArrayPipe(struct, bufsize=20):
    """A simple pipe-like IPC mechanism based on shared memory and a pipe
    to pass a pointer to that shared memory
    
    This class was a precursor to the array_queue module which now supercedes it.
    """
    buffer = RawArray(struct, int(bufsize))
    a,b = Pipe()
    sender = ConnectionSender(buffer, a)
    receiver = ConnectionReceiver(buffer, b)
    return sender, receiver
    

class ConnectionSender(object):
    def __init__(self, buffer, pipe):
        self.buffer = numpy.frombuffer(buffer, dtype=numpy.dtype(buffer._type_))
        self.pipe = pipe
        self.stock = set(range(len(self.buffer)))
        
    def send(self, scalar):
        p = self.pipe
        s = self.stock
        while p.poll() or not s:
            returned = p.recv()
            #print "returned A", returned
            s.add(returned)
        try:
            idx = s.pop()
        except KeyError:
            idx = p.recv()
        self.buffer[idx] = scalar
        #print "sending A", idx
        p.send(idx)
        
    def close(self):
        self.pipe.send(None)
        self.pipe.close()
    
    
class ConnectionReceiver(object):
    def __init__(self, buffer, pipe):
        self.buffer = numpy.frombuffer(buffer, dtype=numpy.dtype(buffer._type_))
        self.buffer.flags.writeable = False
        self.pipe = pipe
        self.map = {}
        #print "set up"
        
    def finalise(self, r):
        p = self.pipe
        idx,R = self.map.pop(id(r))
        #print "returning", idx
        p.send(idx)
        
    def recv(self):
        p = self.pipe
        idx = p.recv()
        #print "receiving B", idx
        if idx is None:
            raise EOFError
        value = self.buffer[idx]
        #print "ID", id(value), sys.getrefcount(value)
        r = weakref.ref(value, self.finalise)
        self.map[id(r)]=(idx, r)
        #print "sending B", idx
        #p.send(idx)
        return value
