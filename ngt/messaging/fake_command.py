#!/usr/bin/env python
import random
import sys

r = random.randint(0,1)
if r == 0:
    print "I am total failure :("
    sys.exit(1)
else:
    print "Great Success!!"
    sys.exit(0)
