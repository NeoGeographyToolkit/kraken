#!/usr/bin/env python

import urllib
from BeautifulSoup import BeautifulSoup

SHOWCASE_URL = 'http://hirise.lpl.arizona.edu/releases/captions_may_10.php?page='
SPOTLIGHT_FILE = 'hirise_spotlight.txt'


def yank_id(element):
    id = element.find('a')['href'] # href of the first child link
    id = id.replace('../','')
    return id.strip()

def scrape_showcase(url):
    ''' Scrape product IDs of featured images from the LPL hirise website '''
    product_ids = []
    page = 0
    while True:
        page += 1
        #print "Grabbing %s" % (url+str(page),)
        soup = BeautifulSoup(urllib.urlopen(url+str(page)))
        thumb_tds = soup.findAll('td', 'desimages') # grabs td.desimages, which contain thumnails and links to the featured images.
        if len(thumb_tds) < 1: break # we're past the last page

        for td in thumb_tds:
            product_ids.append(yank_id(td))
    return product_ids

def read_ids_from_file(filename):
    file = open(filename,'r')
    for line in file:
        yield line.strip()

if __name__ == '__main__':
    site_ids = set(str(id) for id in scrape_showcase(SHOWCASE_URL) )
    file_ids = set(str(id) for id in read_ids_from_file(SPOTLIGHT_FILE))
    for id in  site_ids | file_ids:
        print id
