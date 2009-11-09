#!/usr/bin/env python
# encoding: utf-8
"""
tracker.py

Utility for tracking long iterative processes.
It can be used to wrap an iterator.
"""

import sys
import os
from datetime import datetime

class Tracker(object):
    """
    Tracks the progress of a given iterator.
    """
    def __init__(self, 
        name="TRACKER", 
        report_every=1, 
        target=None, 
        iter=None, 
        progress=False,
        output_to=sys.stderr
    ):
        if progress:
            assert target != None
            if target == 0:
                raise ValueError("Tracker can't deal with a target of 0.")
        self.count = 0
        self.output_stream = output_to
        self.name = name
        self.iter = iter.__iter__() if iter != None else None
        if not target and iter and hasattr(iter, '__len__'):
            self.target = len(iter)
        else:
            self.target = target
        self.progress = progress
        self.report_every = report_every
        self.starttime = datetime.now()
    
    def _report_spew(self):
        if self.target:
            remaining = (datetime.now() - self.starttime) / self.count * (self.target - self.count)
            self.output_stream.write( "%s: %d of %d done. (%s remaining)\n" % (self.name, self.count, self.target, str(remaining)) )
        else:
            self.output_stream.write( "%s: %d done. (%s)\n" % (self.name, self.count, str(datetime.now() - self.starttime) ) )
            
    def _report_bar(self):
        scale = 80
        barlength = int(float(self.count) / float(self.target) * scale)
        #self.output_stream.write("\r"+''.join(( '=' for i in range(1,barlength)))+'>'+''.join((' ' for i in range(1,scale-barlength-1))) + " %d"%count)
        self.output_stream.write("\r[%s>%s]%d" % (''.join(['=' for i in range(1,barlength)]), ''.join([' ' for i in range(1,scale - barlength)]), self.count))
        self.output_stream.flush()
        if self.count == self.target: # last one...
            self.output_stream.write("\n")
            self.output_stream.flush()
    
    def _report(self):
        if self.progress:
            self._report_bar()
        else:
            self._report_spew()
    
    def next(self):
        self.count += 1
        self.count += 1
        if self.count % self.report_every == 0:
            if not self.target or self.count <= self.target:
                self._report()
        if self.iter:
            return self.iter.next()
        else:
            return self.count
            
    def __iter__(self):
        return self
        
        

def test():
    "Just a quick fuctional test."
    import time
    t=Tracker(target=1000, iter=range(1000), progress=True)
    for i in t:
        time.sleep(0.005)


if __name__ == '__main__':
    test()

