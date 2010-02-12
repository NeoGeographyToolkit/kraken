#!/usr/bin/env python

import optparse
import sys, os, os.path
import re
import math
from subprocess import Popen, PIPE

DEFAULT_TMP_DIR = '/tmp/'

def which(command):
    return Popen(('which', command), stdout=PIPE).stdout.read().strip()
    
externals = {}
for command in ('gdal_translate','kdu_expand','hirise2tif','img2plate'):
    externals[command] = which(command)
    if not externals[command]:
        raise Exception("Could not find %s in $PATH." % command)


class Observation(object):
    ''' Carries the four relevant file paths for a single observation. '''
    propmap = {
        'RED.JP2': 'red_image',
        'RED.LBL': 'red_label',
        'COLOR.JP2': 'color_image',
        'COLOR.LBL': 'color_label'
    }

    def __init__(img_path):
        self.path = img_path
        if self.path[-1] == '/':
            self.obsid = os.path.basename(self.path[:-1])
        else:
            self.obsid = os.path.basename(self.path)
        files = os.listdir(img_path)
        for file in files:
            key = file.split('_')[-1]
            if key in propmap:
                self.__dict__[propmap[key]] = os.path.join(self.img_path, file)
            for propname in propmap.values():
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
   

    def __init__(self,label_file):
        self.file = label_file
        info = open(self.file, 'r')
        for line in info:
            m = proj_type_re.match( line )
            if m: self.proj_type = m.group(1)
            m = radius_re.match( line )
            if m: self.radius = float(m.group(1))*1000
            m = center_lat_re.match( line )
            if m: self.center_lat = float(m.group(1))
            m = center_lon_re.match( line )
            if m: self.center_lon = float(m.group(1))
            m = rows_re.match( line )
            if m: self.rows = int(m.group(1))
            m = cols_re.match( line )
            if m: self.cols = int(m.group(1))
            m = scale_re.match( line )
            if m: self.scale = float(m.group(1))
            m = line_offset_re.match( line )
            if m: self.line_offset = float(m.group(1))
            m = sample_offset_re.match( line )
            if m: self.sample_offset = float(m.group(1))
            m = max_lat_re.match( line )
            if m: self.max_lat = float(m.group(1))
            m = min_lat_re.match( line )
            if m: self.min_lat = float(m.group(1))
            m = east_lon_re.match( line )
            if m: self.east_lon = float(m.group(1))
            m = west_lon_re.match( line )
            if m: self.west_lon = float(m.group(1))
            
            
def generate_tif(jp2_path, label_path):
    
    # Parameters: name, geogcs, projection, center_lat ; radius
    projcs = 'PROJCS["%s",%s,PROJECTION["%s"],PARAMETER["latitude_of_origin",%f],PARAMETER["central_meridian",0],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]]]'
    geogcs = 'GEOGCS["Geographic_Coordinate_System",DATUM["unknown",SPHEROID["unnamed",%f,0]],PRIMEM["Prime_Meridian",0],UNIT["degree",0.0174532925199433]]'

    info = Label(label_path)

    jp2 = jp2_path
    (root, ext) = os.path.splitext(os.path.basename(jp2_path))
    tif = os.path.join(options.tmpdir, root + '.tif')
    kdu_tif = os.path.join(options.tmpdir, root + '.kdu.tif')

    '''
    if os.path.exists(tif):
        print '(Using existing ' + tif + ')'
        return tif

    if os.path.exists( kdu_tif ):
        print '(Using existing ' + kdu_tif + ')'
    else:
        cmd = '%s -i %s -o %s' % (kdu_expand_path, jp2, kdu_tif)
        print cmd
        os.system(cmd)
    '''
    
    cmd = '%s -i %s -o %s' % (externals['kdu_expand'], jp2, kdu_tif)
    print cmd
    exit_status = os.system(cmd)
    if exit_status != 0:
        raise Exception("kdu_expand failed!")

    if info.proj_type == 'POLAR STEREOGRAPHIC':
        if info.center_lat == 90:
            srs = projcs % ('North_Polar_Stereographic',(geogcs%(info.radius,)),'Polar_Stereographic',info.center_lat)
        elif info.center_lat == -90:
            srs = projcs % ('South_Polar_Stereographic',(geogcs%(info.radius,)),'Polar_Stereographic',info.center_lat)
        else:
            raise ValuError('Unsupported center latitude in polar stereographic projection')
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

    cmd = '%s -of GTiff -co TILED=YES -co BIGTIFF=YES -co COMPRESS=LZW -a_srs %s -a_ullr %f %f %f %f %s %s' % (externals['gdal_translate'],srs.replace('"','\\"'),ulx,uly,lrx,lry,kdu_tif,tif)
    print cmd
    exit_status = os.system(cmd)
    if exit_status != 0:
        raise Exception("gdal_translate failed!")
    os.unlink(kdu_tif)
    return tif
    
def make_intermediates(obs):
    red_tif = generate_tif(obs.red_image, obs.red_label)
    if obs.color_image:
        assert obs.color_label
        color_tif = generate_tif(obs.color_image, obs.color_label)
    else:
        color_tif = ''
    return (red_tif, color_tif)

def make_geotiff(obs, alpha=True):
    if alpha:
        tif = os.path.join(options.tmpdir, obs.obsid, '.alpha.tif')
    else:
        tif = os.path.join(options.tmpdir, obs.obsid, '.tif')
    '''
    if os.path.exists(tif):
        print '(Using existing ' + tif + ')'
        return
    '''
    (red_tif, color_tif) = make_intermediates(obs)
    cmd = '%s %s %s -o %s' % (externals['hirise2tif'],red_tif,color_tif,tif)
    if alpha:
        cmd = cmd + ' --alpha'
    print cmd
    exit_status = os.system(cmd)
    if exit_status != 0:
        raise Exception("hirise2tif failed!")
    os.unlink(red_tif)
    if color_tif: os.unlink(color_tif)
    return tif

def img2plate(imagefile, platefile):
    cmd = [externals['img2plate']]
    if options.transaction_id:
        cmd += ('-t', options.transaction_id)
    cmd += ('-o', platefile, imagefile)
    cmd = ' '.join(cmd)
    exit_status = os.system(cmd)
    if exit_status != 0:
        raise Exception("img2plate failed!")
    
if __name__ == '__main__':
    global options
    parser = optparse.OptionParser()
    parser.add_option('--tmp', action='store', dest='tmpdir')
    parser.add_option('-t', '--transaction-id', action='store', dest='transaction_id', type='int')
    parser.set_defaults(tmpdir=DEFAULT_TMP_DIR, transaction_id=None)
    parser.set_usage("Usage: %prog [options] observation_path platefile")
    try:
        (options, args) = parser.parse_args()
        (source_path, platefile) = args
    except ValueError: # len(args) < 2
        parser.print_usage()
        sys.exit(2)
    
    obs = Observation(source_path)
    geotiff = make_geotiff(obs)
    img2plate(geotiff, platefile)
    os.unlink(geotiff)
    print "Mipmap successful!"
    sys.exit(0)