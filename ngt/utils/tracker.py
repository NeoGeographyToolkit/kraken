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
    def __init__(self, name="TRACKER", report_every=10, target=None, iter=None, progress=False):
        if progress:
            assert target
        self.count = 0
        self.name = name
        self.target = target
        self.progress = progress
        self.report_every = report_every
        self.starttime = datetime.now()
        self.iter = iter.__iter__() if iter else None
    
    def _report_dump(self):
        if self.target:
            remaining = (datetime.now() - self.starttime) / self.count * (self.target - self.count)
            print "%s: %d of %d done. (%s remaining)" % (self.name, self.count, self.target, str(remaining))
        else:
            print "%s: %d done. (%s)" % (self.name, self.count, str(datetime.now() - self.starttime) )
            
    def _report_bar(self):
        scale = 80
        barlength = int(float(self.count) / float(self.target) * scale)
        #sys.stdout.write("\r"+''.join(( '=' for i in range(1,barlength)))+'>'+''.join((' ' for i in range(1,scale-barlength-1))) + " %d"%count)
        sys.stdout.write("\r%s>%s %d" % (''.join(['=' for i in range(1,barlength)]), ''.join([' ' for i in range(1,scale - barlength)]), self.count))
        sys.stdout.flush()
    
    def _report(self):
        if self.progress:
            self._report_bar()
        else:
            self._report_dump()
    
    def next(self):
        self.count += 1
        if self.count % self.report_every == 0:
            self._report()
        if self.iter:
            return self.iter.next()
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

