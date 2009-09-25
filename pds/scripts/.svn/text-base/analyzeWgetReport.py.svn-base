#!/usr/bin/env python2.5

import os
import sys
import tarfile

if __name__ == "__main__":
  tarrepos = None
  if sys.argv[1:]:
    repository = sys.argv[1]
  else:
    sys.exit(0)

  try:
    tarrepos = tarfile.open(repository,'r:*') # open tar file with transparent compression
    # TODO: implement error reporting from the progress file log!
  finally:
    if tarrepos is not None:
      tarrepos.close()
