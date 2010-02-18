import time, os.path, glob
from pds.ingestion.cum_index import Table
from ngt.utils.tracker import Tracker
from assets.models import Asset

from django.db import transaction

moc_meta_path = '/big/sourcedata/moc/meta'
moc_rootpath='/big/assets/mocsource/'

index_files = [
    #('ab1_sp2.lbl', 'ab1_sp2.tab', 2018), # no data_quality_desc field here...
    ('fha_s10.lbl', 'fha_s10.tab', 210078),
]

def generate_volnames():
    for file in glob.glob(os.path.join(moc_rootpath,'mgsc_1*')):
        if os.path.isdir(file):
            yield file
            
def generate_index_rows():
    for volname in Tracker(list(generate_volnames()), progress=True):
        index_path = os.path.join(moc_rootpath, volname, 'index')
        labelfile = os.path.join(index_path, 'imgindex.lbl')
        tabfile = os.path.join(index_path, 'imgindex.tab')
        tbl = Table(labelfile, tabfile)
        for row in tbl:
            yield row

def build_index():
    print "Building Index"
    index = {}
    t0 = time.time()
    for lbl, tab, count in index_files:
        lbl = os.path.join(moc_meta_path, lbl)
        tab = os.path.join(moc_meta_path, tab)
        table = Table(lbl,tab)
        #for row in Tracker(table, target=count, progress=True):
        for row in generate_index_rows():
            index[row.product_id] = row.data_quality_desc
    print "Finised in %f sec." % (time.time() - t0)
    
    return index
    
@transaction.commit_on_success
def main():
    index = build_index()
    
    mocprocd_assets = Asset.objects.filter(class_label='mocprocd image')
    print "Fixing %d mocproc'd assets" % mocprocd_assets.count()
    i = 0
    for asset in Tracker(mocprocd_assets, target=mocprocd_assets.count(), progress=True):
        try:
            if index[asset.product_id].strip() == 'ERRORS':
                asset.has_errors = True
                asset.save()
                i += 1
        except KeyError:
            print "Error: %s not in index!" % asset.product_id
    print "%d assets have errors" % i
    
    int8_assets = Asset.objects.filter(class_label='scaled image int8')
    print "Fixing %d int8 assets" % int8_assets.count()
    i = 0
    for asset in Tracker(int8_assets, target=int8_assets.count(), progress=True):
        asset.has_errors = asset.creator_job.assets.all()[0].has_errors
        if asset.has_errors:
            asset.save()
            i += 1
    print "%d assets have errors" % i
    print "Done!"
    
