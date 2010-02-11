import sys
import os.path
from django.db import transaction
from assets.models import Asset, DATA_ROOT
import sqlite3 as sqlite

BASEDIR = '/big/sourcedata/hirise/PDS/'
CHECKLIST_DBFILE = '/big/sourcedata/hirise/PDS/checklist.sqlite'

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
        path = os.path.join(BASEDIR, url.partition('PDS/')[-1])
        product_id = path.split('/')[-2]
        filetype = path.split('_')[-1]
        if product_id not in self.products:
            self.assets[product_id] = (path, md5_check, False for i in range(4)])
        self.products[product_id][1][self.filetypes[filetype]] = True
        

@transaction.commit_on_success
def create_assets():
    
    print "INIT."
    records = get_records()
    inventory = HiRiseAssetInventory()

    print "Scanning download tracking DB."
    for record in Tracker(records, 100):
        url, md5_check = record
        inventory.add(url)
    
    print "Creating Assets."
    for (product_id, (path, mdcheck, fileflags)) in Tracker(inventory.assets.items(), progress=True):
        f = fileflags
        assetize_this = False
        if f[0] and verify(f[0]):
            if f[1] and verify(f[1]):
                assetize_this = True
            else:
                error("Metadata missing: product %s" % product_id)
        if f[2] and verify(f[2]):
            if f[3] and verify(f[3]):
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
            
    
def make_asset(product_id, path, md5_checl):
    asset = Asset()
    asset.class_label = 'hirise product'
    asset.instrument_name = 'HiRISE'
    asset.product_id = product_id
    asset.md5_check = md5_check
    asset.relative_file_path = path.partition('DATA_ROOT')[-1]
    asset.save()
    
    

errfile = open('asset_errors.log', 'w')    
def error(errstr):
    print errstr
    errfile.write(errstr)