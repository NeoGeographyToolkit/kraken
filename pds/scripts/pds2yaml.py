#!/usr/bin/env python2.6
# v1.2 8/31/09, Frank Kuehnel, code refactoring into PdsParser class
# Copyright, NASA/RIACS, NASA/SGT

import sys
import os

import yaml

sys.path.insert(0, os.path.abspath('../'))
from parser.pds_serializer import PdsParser

usage =""" pds2yaml file
Analyzes the pds label structure from a file and outputs the corresponding YAML structure to stdout.
"""

if __name__ == "__main__":
  pds_stream = None
  try:
    if sys.argv[1:]:
      pds_filename = sys.argv[1]
      try:
        options = sys.argv[2]
        is_viking = True
      except IndexError:
        is_viking = False
      fsize = os.stat(pds_filename).st_size
      #print "file \'%s\' with size %d" % (pds_filename, fsize)
      pds_stream = open(pds_filename, 'rb') # open and read as binary
      pdsLabel = PdsParser(pds_stream)

      hash = pdsLabel.process(pds_filename, is_viking)
      hash.setdefault('resourceURL', [pds_filename, fsize])

      # use only standard YAML representations
      dump = yaml.safe_dump(hash)
      sys.stdout.write(dump)
    else:
      usage

  except Exception, e:
    print repr(e)

  finally:
    if pds_stream is not None:
      pds_stream.close()

