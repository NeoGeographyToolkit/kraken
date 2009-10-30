import sys, os, glob
import md5

from django.core.management import setup_environ
from ngt import settings
setup_environ(settings)

from pds.ingestion.cum_index import Table
from ngt.assets.models import Asset
from ngt.utils.tracker import Tracker

rootpath='/big/sourcedata/moc'

def generate_volnames():
    for file in glob.glob(os.path.join(rootpath,'mgsc_1*')):
        if os.path.isdir(file):
            yield file
            
def generate_image_records():
    for volname in generate_volnames():
        index_path = os.path.join(rootpath, volname, 'index')
        labelfile = os.path.join(index_path, 'imgindex.lbl')
        tabfile = os.path.join(index_path, 'imgindex.tab')
        tbl = Table(labelfile, tabfile)
        for row in tbl:
            yield (volname.split('/')[-1], row)
            
def peek_at_records():
    import pdb
    for rec in generate_image_records():
        pdb.set_trace()
        continue

latitude_fields = ('lower_right_latitude','lower_left_latitude','upper_left_latitude','upper_left_latitude')
def update_latititudes():
    for volname, rec in Tracker(name='assets', iter=generate_image_records()):
        asset = Asset.objects.get(product_id=rec.product_id, volume=volname.upper())
        asset.center_latitude = rec.center_latitude
        corner_latitudes = [ getattr(rec, a) for a in latitude_fields]
        asset.min_latitude = min(corner_latitudes)
        asset.max_latitude = max(corner_latitudes)
        asset.save()

if __name__ == '__main__':
    peek_at_records()
