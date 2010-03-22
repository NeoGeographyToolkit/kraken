#!/usr/bin/env python

import optparse
import sys, os, os.path
import re
import math
import subprocess, shlex

try:
    from termcolor import colored
except ImportError:
    def colored(value, *unused_args, **unused_kw):
        return value


DEFAULT_TMP_DIR = '/scratch/tmp'
KDU_EXPAND_THREADS = 4

VWBIN_DIR = '/home/mbroxton/projects/visionworkbench/build/bin'
#VWBIN_DIR = '/big/software/visionworkbench/bin'
if VWBIN_DIR not in os.environ['PATH']:
    os.putenv('PATH', VWBIN_DIR + ':' + os.environ['PATH'])

def which(command):
    return subprocess.Popen(('which', command), stdout=subprocess.PIPE).stdout.read().strip()
    
externals = {}
for command in ('gdal_translate','kdu_expand','hirise2tif','image2plate'):
    externals[command] = which(command)
    if not externals[command]:
        raise Exception("Could not find %s in $PATH." % command)

def unlink_if_exists(filepath):
    try:
        os.unlink(filepath)
    except OSError as err:
        if err.errno == 2: # file doesn't exist
            pass  # ignore ignore the error
        else:
            raise err

class Observation(object):
    ''' Carries the four relevant file paths for a single observation. '''
    propmap = {
        'RED.JP2': 'red_image',
        'RED.LBL': 'red_label',
        'COLOR.JP2': 'color_image',
        'COLOR.LBL': 'color_label'
    }

    def __init__(self, img_path):
        self.path = img_path
        if self.path[-1] == '/':
            self.obsid = os.path.basename(self.path[:-1])
        else:
            self.obsid = os.path.basename(self.path)
        files = os.listdir(img_path)
        for file in files:
            key = file.split('_')[-1]
            if key in self.propmap:
                self.__dict__[self.propmap[key]] = os.path.join(self.path, file)
                print "%s matched key %s" % (file, key)
            else:
                print "No match for %s" % file
        for propname in self.propmap.values():
            if propname not in self.__dict__:
                print "%s file not found" % propname
                self.__dict__[propname] = None

class Label(object):
    ''' 
        Parses a PDS label and extracts the relevant metadata. 
        This parser was ripped still-beating from the chest of hirise.py in the googlenasa repository.
    '''
    proj_type_re = re.compile(r' *MAP_PROJECTION_TYPE *= *"(.*)"')
    radius_re = re.compile(r' *C_AXIS_RADIUS *= *([0-9.]*)')
    center_lat_re = re.compile(r' *CENTER_LATITUDE *= *([0-9.-]*)')
    center_lon_re = re.compile(r' *CENTER_LONGITUDE *= *([0-9.-]*)')
    rows_re = re.compile(r' *LINE_LAST_PIXEL *= *([0-9]*)')
    cols_re = re.compile(r' *SAMPLE_LAST_PIXEL *= *([0-9]*)')
    scale_re = re.compile(r' *MAP_SCALE *= *([0-9.]*)')
    line_offset_re = re.compile(r' *LINE_PROJECTION_OFFSET *= *([0-9.-]*)') 
    sample_offset_re = re.compile(r' *SAMPLE_PROJECTION_OFFSET *= *([0-9.-]*)') 
    min_lat_re = re.compile(r' *MINIMUM_LATITUDE *= *([0-9.-]*)')
    max_lat_re = re.compile(r' *MAXIMUM_LATITUDE *= *([0-9.-]*)')
    east_lon_re = re.compile(r' *EASTERNMOST_LONGITUDE *= *([0-9.-]*)')
    west_lon_re = re.compile(r' *WESTERNMOST_LONGITUDE *= *([0-9.-]*)')
    min_stretch1_re = re.compile(r' *MRO:MINIMUM_STRETCH *= *([0-9]*)')
    max_stretch1_re = re.compile(r' *MRO:MAXIMUM_STRETCH *= *([0-9]*)')
    min_stretch3_re = re.compile(r' *MRO:MINIMUM_STRETCH *= *\(*([0-9]*), ([0-9]*), ([0-9]*)\)*')
    max_stretch3_re = re.compile(r' *MRO:MAXIMUM_STRETCH *= *\(*([0-9]*), ([0-9]*), ([0-9]*)\)*')

    def __init__(self,label_file):
        self.file = label_file
        info = open(self.file, 'r')
        for line in info:
            m = self.proj_type_re.match( line )
            if m: self.proj_type = m.group(1)
            m = self.radius_re.match( line )
            if m: self.radius = float(m.group(1))*1000
            m = self.center_lat_re.match( line )
            if m: self.center_lat = float(m.group(1))
            m = self.center_lon_re.match( line )
            if m: self.center_lon = float(m.group(1))
            m = self.rows_re.match( line )
            if m: self.rows = int(m.group(1))
            m = self.cols_re.match( line )
            if m: self.cols = int(m.group(1))
            m = self.scale_re.match( line )
            if m: self.scale = float(m.group(1))
            m = self.line_offset_re.match( line )
            if m: self.line_offset = float(m.group(1))
            m = self.sample_offset_re.match( line )
            if m: self.sample_offset = float(m.group(1))
            m = self.max_lat_re.match( line )
            if m: self.max_lat = float(m.group(1))
            m = self.min_lat_re.match( line )
            if m: self.min_lat = float(m.group(1))
            m = self.east_lon_re.match( line )
            if m: self.east_lon = float(m.group(1))
            m = self.west_lon_re.match( line )
            if m: self.west_lon = float(m.group(1))
            m = self.min_stretch1_re.match( line )
            if m and len(m.group(1)): self.min_stretch = [ int(m.group(1)) ]
            m = self.max_stretch1_re.match( line )
            if m and len(m.group(1)): self.max_stretch = [ int(m.group(1)) ]
            m = self.min_stretch3_re.match( line )
            if m: self.min_stretch = [ int(m.group(1)), int(m.group(2)), int(m.group(3)) ]
            m = self.max_stretch3_re.match( line )
            if m: self.max_stretch = [ int(m.group(1)), int(m.group(2)), int(m.group(3)) ]
            
def execute(cmdstr, display=True, pretend=None):
    ''' Execute the specified command and return its exit status. '''
    if pretend is None or options.dry_run:
        pretend = options.dry_run
    if display:
        print colored('Running command: %s\n' % (cmdstr,), 'blue', attrs=['bold'])

    if pretend:
        return 0
    else:
        return subprocess.call(shlex.split(cmdstr), stderr=subprocess.STDOUT)

def generate_tif(jp2_path, label_path):
    
    # Parameters: name, geogcs, projection, center_lat ; radius
    projcs = 'PROJCS["%s",%s,PROJECTION["%s"],PARAMETER["latitude_of_origin",%f],PARAMETER["central_meridian",0],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]'
    geogcs = 'GEOGCS["Geographic_Coordinate_System",DATUM["unknown",SPHEROID["unnamed",%f,0]],PRIMEM["Prime_Meridian",0],UNIT["degree",0.0174532925199433]]'

    print 'Parsing PDS Label information...'
    info = Label(label_path)

    # Extract stretch parameters
    print '\t--> Min stretch: ' + str(info.min_stretch)
    print '\t--> Max stretch: ' + str(info.max_stretch)
    stretch = (info.min_stretch, info.max_stretch)
    
    jp2 = jp2_path
    print jp2
    (root, ext) = os.path.splitext(os.path.basename(jp2_path))
    # tif = os.path.join(options.tmpdir, root + '.tif')
    # kdu_tif = os.path.join(options.tmpdir, root + '.kdu.tif')

    '''
    if os.path.exists(tif):
        print '(Using existing ' + tif + ')'
        return tif

    if os.path.exists( kdu_tif ):
        print '(Using existing ' + kdu_tif + ')'
    else:
        cmd = '%s -i %s -o %s' % (kdu_expand_path, jp2, kdu_tif)
        print cmd
        execute(cmd)
    '''
    
    #cmd = '%s -fprec 16L -num_threads %d -i %s -o %s' % (externals['kdu_expand'],
#                                                         KDU_EXPAND_THREADS,
#                                                         jp2, kdu_tif) # oversample to 16 bits
    #print cmd
    #exit_status = 0 #execute(cmd)
    #if exit_status != 0:
    #    unlink_if_exists(kdu_tif)
    #    raise Exception("kdu_expand failed!")

    # Extract georeferencing parameters
    if info.proj_type == 'POLAR STEREOGRAPHIC':
        if info.center_lat == 90:
            srs = projcs % ('North_Polar_Stereographic',(geogcs%(info.radius,)),
                            'Polar_Stereographic',info.center_lat)
        elif info.center_lat == -90:
            srs = projcs % ('South_Polar_Stereographic',(geogcs%(info.radius,)),
                            'Polar_Stereographic',info.center_lat)
        else:
            raise ValueError('Unsupported center latitude in polar stereographic projection')
        scalex = info.scale
        scaley = info.scale
    elif info.proj_type == 'EQUIRECTANGULAR':
        srs = geogcs % (info.radius,)
        res = math.pi / 180.0 * info.radius / info.scale
        scalex = 1/res/math.cos( math.pi/180.0*info.center_lat )
        scaley = 1/res
    else:
        raise ValueError('Unsupported projection type')
    ulx = -scalex * info.sample_offset + info.center_lon
    uly = scaley * info.line_offset
    lrx = -scalex * info.sample_offset + scalex * info.cols + info.center_lon
    lry = scaley * info.line_offset - scaley * info.rows

    wkt = srs.replace('"','\\"')
    ullr = [ulx,uly,lrx,lry]

    # Return final results
    return (jp2, stretch, wkt, ullr)
    
def make_geotiff(obs, alpha=True):
    if alpha:
        tif = os.path.join(options.tmpdir, obs.obsid + '.alpha.tif')
    else:
        tif = os.path.join(options.tmpdir, obs.obsid + '.tif')
    '''
    if os.path.exists(tif):
        print '(Using existing ' + tif + ')'
        return
    '''

    # Make intermediates using kdu_expand
    (red_jp2, red_stretch, red_wkt, red_ullr) = generate_tif(obs.red_image, obs.red_label)
    if obs.color_image:
        assert obs.color_label
        (color_jp2, color_stretch, color_wkt, color_ullr) = generate_tif(obs.color_image,
                                                                         obs.color_label)

    # Build the hirise2tif command line (it's long!!)
    if (obs.color_image): 
        cmd = '%s %s %s --stats \"%d,%d;%d,%d;%d,%d;%d,%d\" --wkt-gray \"%s\" --wkt-color \"%s\" --ullr-gray %0.9f,%0.9f,%0.9f,%0.7f --ullr-color %0.9f,%0.9f,%0.9f,%0.9f -o %s' % (externals['hirise2tif'],
                                                                                                                                                                                red_jp2,color_jp2,
                                                                                                                                                                                red_stretch[0][0],
                                                                                                                                                                                red_stretch[1][0],
                                                                                                                                                                                color_stretch[0][0],
                                                                                                                                                                                color_stretch[1][0],
                                                                                                                                                                                color_stretch[0][1],
                                                                                                                                                                                color_stretch[1][1],
                                                                                                                                                                                color_stretch[0][2],
                                                                                                                                                                                color_stretch[1][2],
                                                                                                                                                                                red_wkt, color_wkt,
                                                                                                                                                                                red_ullr[0], red_ullr[1],
                                                                                                                                                                                red_ullr[2], red_ullr[3],
                                                                                                                                                                                color_ullr[0], color_ullr[1],
                                                                                                                                                                                color_ullr[2], color_ullr[3],
                                                                                                                                                                                tif)
    else: 
        cmd = '%s %s --stats %d,%d;%d,%d;%d,%d;%d,%d --wkt-gray \"%s\" --ullr-gray %0.9f,%0.9f,%0.9f,%0.7f -o %s' % (externals['hirise2tif'],
                                                                                                                     red_jp2,
                                                                                                                     red_stretch[0][0],
                                                                                                                     red_stretch[1][0],
                                                                                                                     0,0,0,0,0,0,
                                                                                                                     red_wkt,
                                                                                                                     red_ullr[0], red_ullr[1],
                                                                                                                     red_ullr[2], red_ullr[3],
                                                                                                                     tif)


    if alpha:
        cmd = cmd + ' --alpha'
    try:
        exit_status = execute(cmd)
        if exit_status != 0:
            unlink_if_exists(tif)
            raise Exception("hirise2tif failed!")
    finally:
        pass
#        if options.delete_files:
#            unlink_if_exists(red_tif)
#        if color_tif and options.delete_files:
#            unlink_if_exists(color_tif)
    return tif

def image2plate(imagefile, platefile):
    cmd = [externals['image2plate']]
    if options.transaction_id:
        cmd += ('-t', str(options.transaction_id))
    cmd += ('--file-type auto -o', platefile, imagefile)
    cmd = ' '.join(cmd)

    exit_status = execute(cmd, pretend=not options.write_to_plate)
    if exit_status != 0:
        raise Exception("image2plate failed!")
    
if __name__ == '__main__':

    global options
    parser = optparse.OptionParser()
    parser.add_option('--tmp', action='store', dest='tmpdir', help="Where to write intermediate images (default: %s)" % DEFAULT_TMP_DIR)
    parser.add_option('-t', '--transaction-id', action='store', dest='transaction_id', type='int')
    parser.add_option('--preserve', '-p', dest='delete_files', action='store_false', help="Don't delete the intermediate files.")
    parser.add_option('--noplate', dest='write_to_plate', action='store_false', help="Like --dry-run, except run everything but image2plate")
    parser.add_option('--dry-run', dest='dry_run', action='store_true', help='Print the commands to be run without actually running them')
    parser.set_defaults(tmpdir=DEFAULT_TMP_DIR, transaction_id=None, delete_files=True, write_to_plate=True, dry_run=False)
    parser.set_usage("Usage: %prog [options] observation_path platefile")
    try:
        (options, args) = parser.parse_args()
        (source_path, platefile) = args
    except ValueError: # len(args) < 2
        parser.print_usage()
        sys.exit(2)
    
    obs = Observation(source_path)
    geotiff = make_geotiff(obs)
    try:
        image2plate(geotiff, platefile)
    finally:
        if options.delete_files:
            unlink_if_exists(geotiff)
    print "Mipmap successful!"
    sys.exit(0)
