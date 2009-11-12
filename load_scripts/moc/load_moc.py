import sys, os, glob
import md5
from decimal import Decimal

from django.core.management import setup_environ
sys.path.insert(0,'/home/ted/alderaan-wc')
from ngt import settings
setup_environ(settings)

from django.db import transaction
from django.contrib.gis.geos import Point, Polygon, LinearRing

from pds.ingestion.cum_index import Table
from ngt.assets.models import Asset
from ngt.utils.tracker import Tracker

#rootpath='/big/sourcedata/moc'
rootpath='/home/ted/data/moc_meta'
mars_srid = 949900

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
            
def count_records():
    count = 0
    #for r in generate_image_records():
    for volname, rec in Tracker(name='assets', iter=generate_image_records(), report_every=100):
        count += 1
#        if count % 100 == 0:
#            sys.stdout.write("\r%d" % count)
#            sys.stdout.flush()
    return count

def peek_at_records():
    import pdb
    for volname, rec in generate_image_records():
        pdb.set_trace()
        continue

latitude_fields = ('upper_left_latitude','upper_right_latitude','lower_right_latitude','lower_left_latitude')
longitude_fields = ('upper_left_longitude','upper_right_longitude','lower_right_longitude','lower_left_longitude')
def update_latititudes():
    for volname, rec in Tracker(name='assets', iter=generate_image_records()):
        asset = Asset.objects.get(product_id=rec.product_id, volume=volname.upper())
        asset.center_latitude = rec.center_latitude
        corner_latitudes = [ getattr(rec, a) for a in latitude_fields]
        asset.min_latitude = min(corner_latitudes)
        asset.max_latitude = max(corner_latitudes)
        asset.save()

def build_footprint(record):
    ''' take a pds record and return a geos polygon with the image footprint '''
    fieldnames = zip(longitude_fields, latitude_fields)
    points = []
    for lonfield, latfield in fieldnames:
        points.append(Point(getattr(record, lonfield), getattr(record, latfield), srid=mars_srid))
    points.append(points[0])
    
    poly = Polygon(LinearRing(*points) )
    return poly

def print_coords(record):
    fieldnames = zip(longitude_fields, latitude_fields)
    for pair in fieldnames:
        for name in pair:
            print name, ": ", getattr(record, name)

@transaction.commit_on_success
def update_footprints():
    for volname, rec in Tracker(name='assets', iter=generate_image_records(), report_every=100):
        asset = Asset.objects.get(product_id=rec.product_id, volume=volname.upper())
        asset.footprint = build_footprint(rec)
        #asset.instrument_name = rec.instrument_name
        asset.save()
    return True
        

def testpolys():
    for vol, rec in generate_image_records():
        yield rec, build_footprint(rec)
def badpolys():
    for rec, p in testpolys():
        if not p.valid and rec.instrument_name == 'MOC-NA':
           yield rec, p
def checkcoords():
    for vol, rec in generate_image_records():
       p = build_footprint(rec)
       print "Longs: ", ' '.join([str(getattr(rec, f)) for f in longitude_fields]) 
       print ' '.join([str(coord[0]) for coord in p.coords[0]])
       print "Lats: ", ' '.join([str(getattr(rec, f)) for f in latitude_fields]) 
       print ' '.join([str(coord[1]) for coord in p.coords[0]])
