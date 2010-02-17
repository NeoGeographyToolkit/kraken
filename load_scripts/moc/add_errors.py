import time
from pds.ingestion.cum_index import Table
from ngt.utils import Tracker
from assets.models import Asset

moc_meta_path = '/big/sourcedata/moc/meta'
index_files = [
    ('ab1_sp2.lbl', 'ab1_sp2.tab', 2018),
    ('fha_s10.lbl', 'fha_s10.tab', 210078),
]

def build_index():
    print "Building Index"
    index = {}
    t0 = time.time()
    for lbl, tab, count in index_files:
        table = Table(lbs,tab)
        for row in Tracker(table, target=count, progress=True):
            index[row['PRODUCT_ID']] = ROW['DATA_QUALITY_DESC']
    print "Finised in %f sec." % time.time() - t0
    
    return index
    
def main():
    index = build_index()
    
    mocprocd_assets = Asset.objects.filter(class_label='mocprocd image')
    print "Fixing %d mocproc'd assets" % mocprocd_assets.count()
    i = 0
    for asset in Tracker(mocprocd_assets, target=mocprocd_assets.count(), progress=True):
        if index[asset.product_id].strip() == 'ERRORS':
            asset.has_errors = True
            asset.save()
            i += 1
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
    