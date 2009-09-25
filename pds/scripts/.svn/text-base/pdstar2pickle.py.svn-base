#!/usr/bin/env python2.5

import os
import sys
import tarfile
import cPickle as pickle

this_dir = os.path.dirname(__file__)
sys.path.insert(0, os.path.abspath(this_dir+'/../'))
from parser.pds_serializer import PdsParser

usage =""" pdstar2pickle file
Converts all PDS label files in a tar repository to a python pickled hash dictionary to stdout.
"""

if __name__ == "__main__":
  tarrepos = None
  if sys.argv[1:]:
    repository = sys.argv[1]
  else:
    sys.exit(0)

  # TODO: improve on error reporting using urls.csv and progress.log in the TAR repository!
  try:
    tarrepos = tarfile.open(repository,'r:*') # open tar file with transparent compression

    member = tarrepos.next()
    while member:
      pds_filename = member.name
      base, sep, ext = pds_filename.rpartition('.')
      fsize = member.size

      if member.isfile() and ext.lower().endswith('lbl'):
        pds_stream = None
        try:
          pds_stream = tarrepos.extractfile(member)
          pdsLabel = PdsParser(pds_stream)

          hash = pdsLabel.process(pds_filename)
          hash.setdefault('resourceURL', [pds_filename, fsize])

          pickled_hash = pickle.dumps(hash)
          sys.stdout.write("%d\n" % len(pickled_hash))
          sys.stdout.write(pickled_hash)
        except Exception, e:
          # report error
          sys.stderr.write("pickle '%s' with size %d -> %s\n" % (pds_filename, fsize, repr(e)))
          continue
        finally:
          if pds_stream is not None:
            pds_stream.close()

      member = tarrepos.next()

  except Exception, e:
    sys.stderr.write(repr(e)) # report error

  finally:
    if tarrepos is not None:
      tarrepos.close()