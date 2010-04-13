from inventory_hirise import scan_assets, scan_index
import csv, sys
from django.contrib.gis.geos import LinearRing

CENTERPOINT_FILE = '/big/sourcedata/mars/hirise/metadata/HiRISE_coords.csv'
THEME_FILE = '/big/sourcedata/mars/hirise/metadata/HiRISE_themes.csv'

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

def check_angles():
    inventory = scan_assets()
    inventory, missing = scan_index(inventory)
        
    iter = inventory.values()
    for i in range(20):
        row = iter.pop().red_record
        print "%f, %f" % (row.spacecraft_altitude, row.emission_angle)

def get_centerpoints(cp_filename):
    centerpoints = {}
    print "Reading image centerpoints from %s" % cp_filename
    cpreader = csv.reader(open(cp_filename, 'rb'), delimiter='\t')
    cpreader.next() # throw away the header row
    for observation_id, lat, lon in cpreader:
        centerpoints[observation_id] = (lat, lon)
    print "Done."
    return centerpoints

def reduce_themefile(theme_filename):
    themes = {}
    print "Reading Theme File %s" % theme_filename
    themereader = csv.reader(open(theme_filename, 'rb'), delimiter='\t')
    themereader.next() # throw away the header row
    i = 0
    for observation_id, theme, precedence in themereader:
        i += 1
        sys.stderr.write('\r%d' % i)
        if (observation_id, theme) not in themes:
            themes[(observation_id, theme)] = int(precedence)
        elif themes[(observation_id, theme)] > int(precedence):
            themes[(observation_id, theme)] = int(precedence)
    sys.stderr.write('\n')
    def flatten(themes):
        for (observation_id, theme), precedence in themes.items():
            yield (observation_id, theme, precedence)
    return flatten(themes)

def output_themes(output_file, theme_filename=THEME_FILE):
    themes = reduce_themefile(theme_filename)
    outfile = open(output_file, 'w')
    writer = csv.writer(outfile)
    header = ('observation_id','theme','max_precedence')
    writer.writerow(header)
    for tup in themes:
        writer.writerow(tup)
    outfile.close()

def output_metadata(filename):
    inventory = scan_assets()
    inventory, missing = scan_index(inventory)
    centerpoints = get_centerpoints(CENTERPOINT_FILE)
    #themes = scan
    
    print "Outputting to %s" % filename
    outfile = open(filename, 'w')
    metadata_writer = csv.writer(outfile)
    header_line = ','.join((
        'observation_id',
        'latitude',
        'longitude',
        'url',
        'description',
        'image_lines',
    ))
    outfile.write(header_line + "\n")
    for observation_id, observation in inventory.items():
        if observation.red_record:
            record = observation.red_record
        else:
            record = observation.color_record
        assert record
        #centerpoint = centroid(record)       
        try:
            centerpoint = centerpoints[record.observation_id]
        except KeyError:
            print "No centerpoint: %s" % record.observation_id
            continue
        metadata_writer.writerow((
            record.observation_id,
            centerpoint[0],
            centerpoint[1],
            "http://hirise.lpl.arizona.edu/" + record.observation_id,
            record.rationale_desc,
            record.image_lines
        ))
