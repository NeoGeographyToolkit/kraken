#!/usr/bin/env python

"""
This script retrieves a file from a given URL and saves it to the specified file name,
outputting a progress report as it does so.
It's pretty much just a wrapper around urllib.urlretrieve.
At the moment, only GET requests are supported.
"""

import sys, os, os.path
import urllib
import optparse

def sizeof_human(numbytes):
    ''' Human-readable size strings '''
    if numbytes < 0:    # urlretrieve passes -1 for totalsize if it can't get the filesize from the server
        return '???'
    for x in ['bytes','KB','MB','GB','TB','PB']:
        if numbytes < 1024.0:
            return "%3.2f%s" % (numbytes, x)
        numbytes /= 1024.0
    else:
        return "Exabytes!"

def download_file(url, dest_filename):
    ensure_path(dest_filename)
    def _report(blockcount, blocksize, totalsize):
        sys.stderr.write("\r%s of %s retrieved." % (sizeof_human(float(blockcount) * float(blocksize)), sizeof_human(totalsize)))
        sys.stderr.flush()
    print "Downloading: %s --> %s" % (url, dest_filename)
    urllib.urlretrieve(url, dest_filename, _report)

def ensure_path(filepath):
    dirname = os.path.dirname(filepath)
    if dirname and not os.path.exists(dirname):
        print "Directory %s does not exist.  Creating" % os.path.dirname(filepath)
        os.makedirs(os.path.dirname(filepath))
    

def main():
    usage = '%prog url dest_filepath'
    parser = optparse.OptionParser(usage=usage)
    (options, args) = parser.parse_args()
    if len(args) != 2:
        parser.print_help()
        sys.exit(1)

    (url, destination) = args
    download_file(url, destination)


if __name__ == '__main__':
    main()
