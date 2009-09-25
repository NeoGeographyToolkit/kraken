#!/opt/local/bin/python
import random
import sys

r = random.randint(1,10)
if r == 10:
    sys.exit(1)
else:
    sys.exit(0)
