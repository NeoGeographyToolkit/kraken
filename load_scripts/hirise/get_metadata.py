from inventory_hirise import scan_assets, scan_index
import csv
from django.contrib.gis.geos import LinearRing
FIELDS = []

def centroid(index_row):
    r = index_row
    lr = LinearRing(
        (r.corner1_longitude, r.corner1_latitude),
        (r.corner2_longitude, r.corner2_latitude),
        (r.corner3_longitude, r.corner3_latitude),
        (r.corner4_longitude, r.corner4_latitude),
        (r.corner1_longitude, r.corner1_latitude),
    )
    return lr.centroid

def output_metadata(filename, fields=FIELDS):
    inventory = scan_assets()
    inventory, missing = scan_index(inventory)
    
    print "Outputting to %s" % filename
    metadata_writer = csv.writer(open(filename, 'w'))
    for observation_id, observation in inventory.items():
        if observation.red_record:
            record = observation.red_record
        else:
            record = observation.color_record
        assert record
        centerpoint = centroid(record)       
        metadata_writer.writerow((
            record.observation_id,
            centerpoint.y,
            centerpoint.x,
            "http://hirise.lpl.arizona.edu/" + record.observation_id,
            record.rationale_desc,
            record.image_lines
        ))
