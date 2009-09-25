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
    def __init__(self, name="TRACKER", report_every=10, target=None, iter=None):
        self.count = 0
        self.name = name
        self.target = target
        self.report_every = report_every
        self.starttime = datetime.now()
        self.iter = iter.__iter__() if iter else None
    
    def report(self):
        if self.target:
            remaining = (datetime.now() - self.starttime) / self.count * (self.target - self.count)
            print "%s: %d of %d done. (%s remaining)" % (self.name, self.count, self.target, str(remaining))
        else:
            print "%s: %d done. (%s)" % (self.name, self.count, str(datetime.now() - self.starttime) )
    
    def next(self):
        self.count += 1
        if self.count % self.report_every == 0:
            self.report()
        if self.iter:
            return self.iter.next()
    def __iter__(self):
        return self
        
        

def test():
    t=Tracker(target=100000)
    for i in range(100000):
        t.next()


if __name__ == '__main__':
    test()

