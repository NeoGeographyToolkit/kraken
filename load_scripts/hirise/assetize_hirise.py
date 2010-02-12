import sys
import os.path
from django.db import transaction
from assets.models import Asset, DATA_ROOT
import sqlite3 as sqlite
from ngt.utils.tracker import Tracker

BASEDIR = '/big/assets/hirise/PDS/'
CHECKLIST_DBFILE = '/big/sourcedata/mars/hirise/checklist.sqlite'

class HiRiseAssetInventory(object):
    filetypes = {
        'RED.JP2': 0,
        'RED.LBL': 1,
        'COLOR.JP2': 2,
        'COLOR.LBL': 3,
    }
    def __init__(self):
        self.products = {} # product_id --> (path, md5_check, [flag,flag,flag,flag]) # flags are filetypes found
        
    def add(self, url, md5_check):
        file_path = os.path.join(BASEDIR, url.partition('PDS/')[-1])
        observation_path = os.path.split(file_path)[0]
        product_id = file_path.split('/')[-2]
        filetype = file_path.split('_')[-1]
        if filetype in self.filetypes:
            if verify(file_path):
                if product_id not in self.products:
                    self.products[product_id] = [observation_path, md5_check, [False for i in range(4)]]
                self.products[product_id][2][self.filetypes[filetype]] = True
                if self.products[product_id][1] and not md5_check:
                    self.products[product_id][1] = False
        

@transaction.commit_on_success
def create_assets():
    
    print "INIT."
    records = get_records()
    inventory = HiRiseAssetInventory()

    print "Scanning download tracking DB."
    for record in Tracker(records, 100, progress=True):
        url, md5_check = record
        inventory.add(url, md5_check)
    
    print "Creating Assets."
    for (product_id, (path, md5_check, fileflags)) in Tracker(inventory.products.items(), progress=True):
        f = fileflags
        assetize_this = False
        if f[0]:
            if f[1]:
                assetize_this = True
            else:
                error("Metadata missing: product %s" % product_id)
        if f[2]:
            if f[3]:
                assetize_this = True
            else:
                error("Metadata missing: product %s" % product_id)            
        else:
            error("No color image: product %s" % product_id)
        
        if assetize_this:        
            make_asset(product_id, path, md5_check)
        else:
            error("Can't make asset for product %s" % product_id)
    print "Done."
            
def get_records():
    # from the download tracking db
    conn = sqlite.connect(CHECKLIST_DBFILE)
    cur = conn.cursor()
    cur.execute("SELECT url, md5_check FROM files;")
    return cur.fetchall()
    
    

def verify(path):
    if os.path.exists(path):
        return True
    else:
        error("FILE NOT FOUND: %s" % path)
        return False
            
    
def make_asset(product_id, path, md5_check):
    asset = Asset()
    asset.class_label = 'hirise product'
    asset.instrument_name = 'HiRISE'
    asset.product_id = product_id
    asset.md5_check = md5_check
    asset.relative_file_path = path.partition(DATA_ROOT)[-1]
    asset.save()
    
    

errfile = open('asset_errors.log', 'w')    
def error(errstr):
    print errstr
    errfile.write(errstr)
