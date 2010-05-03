import sys
import os.path
from django.db import transaction
from assets.models import Asset, DATA_ROOT
from ngt.utils.tracker import Tracker
from pds.ingestion.cum_index import Table
from assetize_hirise import make_asset
from django.db import transaction
import json
import urllib

BASE_URL = 'http://hirise-pds.lpl.arizona.edu/PDS/'
BASEDIR = '/big/assets/hirise/PDS/'
PDSBASEDIR = '/big/sourcedata/mars/hirise/PDS/'
INDEXDIR = '/big/sourcedata/mars/hirise/PDS/INDEX'
INDEXLBL = INDEXDIR + '/RDRCUMINDEX.LBL'
INDEXTAB = INDEXDIR + '/RDRCUMINDEX.TAB'
INDEXLENGTH = 23165
ASSET_CLASS = 'hirise product'
#ERRLOG = sys.stderr
ERRLOG = open('/home/ted/hirise_inventory_error.log','w')

def log_error( message):
    ERRLOG.write(message + "\n")

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

def unlink_if_exists(filepath):
    try:
        os.unlink(filepath)
    except OSError as err:
        if err.errno == 2: # file doesn't exist
            pass  # ignore ignore the error
        else:
            raise err

class Observation(object):
    def __init__(self, initial):
        ''' initalize the observation with either an Asset or a PDS Row instance '''

        self.asset = None
        self.red_record = None
        self.color_record = None
        self.red_image_missing = False
        self.color_image_missing = False

        if type(initial) == Asset:
            self.asset = initial
            self.id = self.asset.product_id
            self.path = self.asset.file_path
        elif type(initial) == Table.Row:
            self.id = initial.observation_id
            self.path = os.path.dirname(initial.file_name_specification)
            self.add_index_record(initial)
        else:
            raise ValueError("Tried to initialize Observation with invalid type: %s" % str(type(initial)))
        self.file_base = self.path + '/' + self.id

    def add_index_record(self, record):
        # record is a Row instance
        if '_RED' in record.product_id:
            self.red_record = record
            if not os.path.exists(self.red_image_file):
                self.red_record_missing = True
                log_error("image file missing: %s" % record.product_id)
        elif '_COLOR' in record.product_id:
            self.color_record = record
            if not os.path.exists(self.color_image_file):
                self.color_record_missing = True
                log_error("image file missing: %s" % record.product_id)
        else:
            raise ValueError("Invalid product_id: %s" % record.product_id)
        

    @property
    def red_image_file(self):
        return self.path + '_RED.JP2'
    @property
    def color_image_file(self):
        return self.path + '_COLOR.JP2'
        
class ObservationInventory(dict):
    ''' keys are observation_ids, values are Observation instances '''
    def __init__(self):
        super(ObservationInventory, self).__init__()

    
    def add_asset(self, asset):
        assert asset.product_id not in self # asset / observation ids should be unique
        self[asset.product_id] = Observation(asset) # for hirise, asset product_ids are really observation_ids!

    def add_index_record(self, record):
        try:
            obs = self[record.observation_id]
        except KeyError:
            log_error("no asset for observation %s" % record.observation_id)
            raise
        obs.add_index_record(record)

def scan_index(labelfile=INDEXLBL, tablefile=INDEXTAB):
    print "Reading cumulative index table %s." % tablefile
    indexlength = linecount(tablefile)
    index = {}
    table = Table(labelfile,tablefile)
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

def compare_to_index(inventory, labelfile=INDEXLBL, tablefile=INDEXTAB):
    ''' Add PDS index rows to an asset inventory where they match. 
        Put them in a seperate list otherwise.
    '''
    print "Reading cumulative index table %s." % INDEXTAB
    indexlength = linecount(tablefile)
    table = Table(labelfile, tablefile)
    missing_products = []
    for row in Tracker(table, target=indexlength, progress=True):
        try:
            inventory.add_index_record(row)
        except KeyError:
            missing_products.append(row)
    return inventory, missing_products

###
# The method below is for replacing HiRISE assets that were accidentally deleted at one point.
# Probably shouldn't be used under normal circumstances.
# 
def replace_missing_files():
    MISSING_FILES_JSON = '/home/ted/failed_product_ids.json'
    BASE_URL = 'http://hirise-pds.lpl.arizona.edu/PDS/'
    missing_files = json.loads(open(MISSING_FILES_JSON, 'r').read())   
    print "%s missing files." % len(missing_files)
    index = scan_index()
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
#
####

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
    def _report(blockcount, blocksize, totalsize):
        sys.stdout.write("\r%s of %s retrieved." % (sizeof_human(blockcount * blocksize), sizeof_human(totalsize)))
    print "%s --> %s" % (url, dest_filename)
    if not os.path.exists(os.path.dirname(dest_filename)):
        print "Directory %s does not exist.  Creating" % os.path.dirname(dest_filename)
        os.makedirs(os.path.dirname(dest_filename))
    urllib.urlretrieve(url, dest_filename, _report)

def index_rows_to_observations(index_rows):
    '''Convert a sequence of index rows to a dict of Observation instances, by observation_id'''
    observations = {}
    for row in index_rows:
        try:
            obs = observations[row.observation_id]
            obs.add_index_record(row)
        except KeyError:
            obs = Observation(row)
            observations[row.observation_id] = obs     
    return observations

@transaction.commit_on_success
def download_pds_products(index_rows):
    ''' Download a set of products given a sequence of PDS index Row instances '''
    observations = index_rows_to_observations(index_rows)
    for observation_id, obs in observations.items():
        for record in (obs.color_record, obs.red_record):
            if record and not os.path.exists(BASEDIR + record.file_name_specification):
                try:
                    download_file(BASE_URL + record.file_name_specification, BASEDIR + record.file_name_specification)
                except:
                    print "Download failed.  Unlinking."
                    unlink_if_exists(BASEDIR + record.file_name_specification)
                    raise

@transaction.commit_on_success
def assetize_pds_products(index_rows):
    ''' Create HiRISE asset records, givin a sequence if PDS Row instances '''
    observations = index_rows_to_observations(index_rows)
    for observation_id, obs in observations.items():

        # double check to make sure files exist
        for record in (obs.color_record, obs.red_record):
            if record: 
                assert os.path.exists(BASEDIR + record.file_name_specification)
                obs_path = os.path.dirname(BASEDIR + record.file_name_specification)

        print "Creating an asset for %s" % observation_id
        make_asset(observation_id, obs_path, True, class_label="fresh hirise product") 

def output_download_list(index_rows, outfilename, jobsetname='download new hirise images'):
    '''Output a list that can be fed to jobs/create_jobset.py to make a download list.'''
    observations = index_rows_to_observations(index_rows)
    print "Writing JobSet load file to %s" % outfilename
    outfile = open(outfilename, 'w')
    outfile.write(jobsetname + '\n')
    for observation_id, obs in observations.items():
        for record in (obs.color_record, obs.red_record):
            if record and not os.path.exists(BASEDIR + record.file_name_specification):
                outfile.write("download %s %s\n" % (BASE_URL + record.file_name_specification, BASEDIR + record.file_name_specification))
    outfile.close()
    print "Done."

def do_inventory():
    inventory = scan_assets()
    inventory, missing_product_rows = compare_to_index(inventory, INDEXLBL, INDEXTAB)
    count = 0
    print "Final inventory pass."
    for obs in Tracker(inventory.values(), progress=True):
        if obs.red_image_missing or obs.color_image_missing:
            count += 1
    print "%d images missing." % count
    print "%d observations not acquired." % len(missing_product_rows)
    return (inventory, missing_product_rows)  # missing_product_rows can be fed to download_pds_products & assetize_pds_products
