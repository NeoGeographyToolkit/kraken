#!/usr/bin/env python

from BeautifulSoup import BeautifulSoup
import urlparse
import sys
import codecs
sys.stdout = codecs.getwriter('utf-8')(sys.stdout)


BASEURL = 'http://hirise.lpl.arizona.edu'

infilename = sys.argv[1]
infile = open(infilename, 'r')
for line in infile:
    (product_id, caption) = line.split('\t')
    product_id = product_id.strip()

    caption = BeautifulSoup(caption)
    for link in caption.findAll('a'):
        url = link.get('href', None)
        if url and 'http' not in url:
            link['href'] = urlparse.urljoin(BASEURL, url)
    sys.stdout.write("%s\t%s" % (product_id, unicode(caption))) 
