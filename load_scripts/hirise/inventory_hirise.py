import sys
import os.path
from django.db import transaction
from assets.models import Asset, DATA_ROOT
from ngt.utils.tracker import Tracker
from pds.ingestion.cum_index import Table
import json
import urllib

BASEDIR = '/big/assets/hirise/PDS/'
PDSBASEDIR = '/big/sourcedata/mars/hirise/PDS/'
INDEXDIR = '/big/sourcedata/mars/hirise/PDS/INDEX'
INDEXLBL = INDEXDIR + '/RDRCUMINDEX.LBL'
INDEXTAB = INDEXDIR + '/RDRCUMINDEX.TAB'
INDEXLENGTH = 23165
ASSET_CLASS = 'hirise product'
#ERRLOG = sys.stderr
ERRLOG = open('/home/ted/hirise_inventory_error.log','w')


def linecount(filename):
    f = open(filename)                  
    lines = 0
    buf_size = 1024 * 1024
    read_f = f.read # loop optimization

    buf = read_f(buf_size)
    while buf:
        lines += buf.count('\n')
        buf = read_f(buf_size)
    f.close()
    return lines

class Observation(object):
    def __init__(self, asset):
        self.id = asset.product_id
        self.path = asset.file_path
        self.file_base = self.path + '/' + self.id
        self.asset = asset

        self.red_record = None
        self.color_record = None
        self.red_image_missing = False
        self.color_image_missing = False

    @property
    def red_image_file(self):
        return self.file_base + '_RED.JP2'
    @property
    def color_image_file(self):
        return self.file_base + '_COLOR.JP2'
        
class ObservationInventory(dict):
    ''' keys are observation_ids, values are Observation instances '''
    def __init__(self):
        super(ObservationInventory, self).__init__()

    def log_error(self, message):
        ERRLOG.write(message + "\n")
    
    def add_asset(self, asset):
        assert asset.product_id not in self # asset / observation ids should be unique
        self[asset.product_id] = Observation(asset) # for hirise, asset product_ids are really observation_ids!

    def add_index_record(self, record):
        try:
            obs = self[record.observation_id]
        except KeyError:
            self.log_error("no asset for observation %s" % record.observation_id)
            return None
        if '_RED' in record.product_id:
            obs.red_record = record
            if not os.path.exists(obs.red_image_file):
                obs.red_record_missing = True
                self.log_error("image file missing: %s" % record.product_id)
        elif '_COLOR' in record.product_id:
            obs.color_record = record
            if not os.path.exists(obs.color_image_file):
                obs.color_record_missing = True
                self.log_error("image file missing: %s" % record.product_id)
        else:
            raise ValueError("Invalid product_id: %s" % record.product_id)

def build_index():
    print "Reading cumulative index table %s." % INDEXTAB
    indexlength = linecount(INDEXTAB)
    index = {}
    table = Table(INDEXLBL,INDEXTAB)
    for row in Tracker(table, target=indexlength, progress=True):   
        index[row.product_id] = row
    return index

def scan_assets():
    assets = Asset.objects.filter(class_label='hirise product')
    inventory = ObservationInventory()
    print "Scanning assets."
    for asset in Tracker(assets, len(assets), progress=True):
        inventory.add_asset(asset)
    return inventory

def scan_index(inventory, labelfile, tablefile):
    print "Reading cumulative index table %s." % INDEXTAB
    indexlength = linecount(tablefile)
    table = Table(labelfile, tablefile)
    for row in Tracker(table, target=indexlength, progress=True):
        inventory.add_index_record(row)
    return inventory

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

def do_inventory():
    inventory = scan_assets()
    inventory = scan_index(inventory, INDEXLBL, INDEXTAB)
    count = 0
    print "Final inventory pass."
    for obs in Tracker(inventory.values(), progress=True):
        if obs.red_image_missing or obs.color_image_missing:
            count += 1
    print "%d images missing." % count
