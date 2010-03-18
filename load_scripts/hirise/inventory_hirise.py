import sys
import os.path
from django.db import transaction
from assets.models import Asset, DATA_ROOT
from ngt.utils.tracker import Tracker
from pds.ingestion.cum_index import Table
import json
import urllib

BASEDIR = '/big/assets/hirise/PDS/'
INDEXDIR = '/big/sourcedata/mars/hirise/PDS/INDEX'
INDEXLBL = INDEXDIR + '/RDRCUMINDEX.LBL'
INDEXTAB = INDEXDIR + '/RDRCUMINDEX.TAB'
INDEXLENGTH = 23165
ASSET_CLASS = 'hirise product'

def build_index():
    print "Reading cumulative index table %s." % INDEXTAB
    index = {}
    table = Table(INDEXLBL,INDEXTAB)
    for row in Tracker(table, target=INDEXLENGTH, progress=True):   
        index[row.product_id] = row
    return index

def replace_missing_files():
    MISSING_FILES_JSON = '/home/ted/failed_product_ids.json'
    BASE_URL = 'http://hirise-pds.lpl.arizona.edu/PDS/'
    missing_files = json.loads(open(MISSING_FILES_JSON, 'r').read())   
    print "%s missing files." % len(missing_files)
    index = build_index()
    for observation_id, observation_path in missing_files.items():
        print "Checking %s" % observation_id
        for postfix in ('_COLOR','_RED'):
            product_id = observation_id + postfix
            if product_id not in index:
                print "%s not found in the index.  skipping." % product_id
                continue
            imgfile =   product_id + '.JP2'
            if os.path.exists(observation_path + '/' + imgfile):
                print "%s EXISTS!" % imgfile
            else:
                print "%s does not exist.  Grabbing it." % imgfile
                url = BASE_URL + index[product_id].file_name_specification
                download_file(url, observation_path + '/' + imgfile)
    print "Done!"

def download_file(url, dest_filename):
    def _report(blockcount, blocksize, totalsize):
        sys.stdout.write("\r%s of %s bytes retrieved." % (blockcount * blocksize, totalsize))
    print "%s --> %s" % (url, dest_filename)
    urllib.urlretrieve(url, dest_filename, _report)
