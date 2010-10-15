#!/usr/bin/env python
import sys, os, os.path
import subprocess
from subprocess import Popen, PIPE
import shlex
import traceback

DEFAULT_TMP_DIR = '/scratch/tmp'
VALIDITY_THRESHOLD = 0.2
#EMISSION_ANGLE_THRESHOLD = 20
EMISSION_ANGLE_THRESHOLD = -1 # emission angle filter disabled

if os.path.dirname(__file__).strip():
    COMMAND_PATH = os.path.abspath(os.path.dirname(__file__))
else:
    COMMAND_PATH = os.path.abspath(os.getcwd())
print "command path is %s" % COMMAND_PATH

class ISISError(Exception):
    pass

def which(command):
    path = Popen(('which', command), stdout=subprocess.PIPE).stdout.read().strip()
    if not path: 
        raise Exception("Could not find %s in $PATH." % command)
    return path

# IMAGE2PLATE = which('image2plate')
IMAGE2PLATE = 'image2plate'

def isis_run(args, message=None, pretend=None, display=None):
    ''' 
        Execute the specified ISIS command and return it's exit status. 
        Use the isis.sh wrapper script to set the correct environment 
    '''
    if pretend == None:
        pretend = options.dry_run
    if display == None:
        display = options.dry_run or options.verbose
    if message:
        print message
    if display:
        print ' '.join(args)
    if not pretend:
        os.chdir('/tmp/')
        p = Popen([os.path.join(COMMAND_PATH, 'isis.sh')]+list(args), shell=False)
        retcode = p.wait()
        if not retcode == 0:
            raise ISISError("%s failed." % args[0])
        return retcode
    else:
        return 0

def execute(cmdstr, display=True, pretend=None):
    ''' Execute the specified command and return its exit status. '''
    if pretend is None or options.dry_run:
        pretend = options.dry_run
    if display:
        print 'Running command: %s\n' % (cmdstr,)

    if pretend:
        return 0
    else:
        return subprocess.call(shlex.split(cmdstr), stderr=subprocess.STDOUT)

def unlink_if_exists(filepath):
    ''' unlink a file if it exists and --preserve is not set. '''
    if options.delete_files:
        try:
            os.unlink(filepath)
        except OSError as err:
            if err.errno == 2: # file doesn't exist
                pass  # ignore ignore the error
            else:
                raise err
    else:
        print "Preserving %s" % filepath

def ctx2isis(ctxfile, cubefile):
    args = [
        'mroctx2isis',
        'from=' + ctxfile,
        'to=' + cubefile
    ]   
    msg = "Converting %s to an ISIS cube." % ctxfile
    retcode = isis_run(args, message=msg)

def spiceinit(cubefile):
    args = [
        'spiceinit',
        'from=' + cubefile
    ]
    retcode = isis_run(args, message="SPICE init.")

def get_percent_valid(file):
    p = Popen([ os.path.join(COMMAND_PATH, 'isis.sh'), 'stats', 'from='+file ], stdout=PIPE)
    stats = p.communicate()[0]
    tokens = stats.split('\n')
    total = None
    valid = None
    for t in tokens:
        subtokens = t.split('=')
        if (len(subtokens) > 1):
            param = subtokens[0].strip()
            if (param == "TotalPixels"):
                total = int(subtokens[1].strip())
            if (param == "ValidPixels"):
                valid = int(subtokens[1].strip())
        if (total is not None) and (valid is not None):
            break
    else:
        raise("Problem getting total and valid pixel counts from ISIS stats.")

    validity = float(valid) / float(total)
    print "valid pixel ratio: %.3f" % validity
    return validity   

def null2lrs(incube, outcube):
    ''' Map all the null pixel values to the low-saturation point (LRS) special value '''
    args = [
        'stretch',
        'null=lrs',
        'hrs=%d' % (2**16 - 1), # doing this to work around an apparent bug in lineeq
        'from=' + incube,
        'to=' + outcube,
    ]
    try:
        retcode = isis_run(args, message="Remapping null values to LRS.")
    finally:
        unlink_if_exists(incube)

def fail_high_emission_angles(file, emission_angle_threshold=20):
    print "Checking Emission Angle...",
    p = Popen([ os.path.join(COMMAND_PATH, 'isis.sh'), 'camstats', 'from='+file, 'linc=100', 'sinc=100' ], stdout=PIPE)
    stats = p.communicate()[0]
    tokens = stats.split('\n')
    for t in tokens:
        subtokens = t.split('=')
        if (len(subtokens) > 1):
            param = subtokens[0].strip()
            if (param == 'EmissionAverage'):
                emission_angle = float(subtokens[1].strip())
                print "%.3f" % emission_angle
                if emission_angle >= emission_angle_threshold:
                    raise Exception("Emission angle too high (%.3f)" % emission_angle)
                else:
                    return 0
    else:
        raise Exception("Failed to get the emission angle.")

# def get_crosstrack_summing(file):
#     p = Popen([ os.path.join(COMMAND_PATH, 'isis.sh'), 'catlab', 'from='+file ], stdout=PIPE)
#     stats = p.communicate()[0]
#     tokens = stats.split('\n')
#     for t in tokens:
#         subtokens = t.split('=')
#         if (len(subtokens) > 1):
#             param = subtokens[0].strip()
#             if (param == 'CrosstrackSumming'):
#                 crosstrack_summing = int(subtokens[1])
#                 return crosstrack_summing
#     else:
#         raise ISISError("Crosstrack Summing value not found in label for %s" % file)

def calibrate(incube, outcube):
    ''' ctxcal and ctxevenodd '''
    ctxcalcube = incube + '.ctxcal.cub'
    try:
        isis_run(('ctxcal', 'from='+incube, 'to='+ctxcalcube), message="Radiometrically calibrating")
        isis_run(('ctxevenodd', 'from='+ctxcalcube, 'to='+outcube), message="Destriping")
    finally:
        unlink_if_exists(ctxcalcube)
        unlink_if_exists(incube)

def lineeq(incube, outcube):
    try:
        isis_run(('lineeq', 'from='+incube, 'to='+outcube), message="Running lineeq.")
    finally:
        unlink_if_exists(incube)

def get_max_camera_latitude(cubefile):
    p = Popen([ os.path.join(COMMAND_PATH, 'isis.sh'), 'camrange', 'from='+cubefile ], stdout=PIPE)
    stats = p.communicate()[0]
    tokens = stats.split('\n')
    max_latitude = None
    for t in tokens:
        subtokens = t.split('=')
        if (len(subtokens) > 1):
            param = subtokens[0].strip()
            if (param == "MaximumLatitude"):
                max_latitude = float(subtokens[1].strip())
                return max_latitude
    else:
        raise ISISError("Couldn't get maximum latitude from camrange.  Cubefile: " + cubefile)


MAPFILES = {
    'PolarStereographic': 'polarstereographic.map',
    'Sinusoidal': 'sinusoidal.map',
}
def map_project(incube, outcube):
    ''' 
        Get the latitude from camrange to determine projection.
        Call cam2map and map-project 
    '''
    max_lat = get_max_camera_latitude(incube)
    if abs(max_lat) >= 85:
        projection = 'PolarStereographic'
    else:
        projection = 'Sinusoidal'
    mapfile = os.path.join(COMMAND_PATH, MAPFILES[projection])
    output_dir = os.path.split(outcube)[0]
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    args = (
        'cam2map',
        'from=' + incube,
        'to=' + outcube,
        'map=' + mapfile
    )
    try:
        retcode = isis_run(args, message="Map projecting, using %s" % projection)
    finally:
        unlink_if_exists(incube)
           

def getminmax(file):
    p = Popen([ os.path.join(COMMAND_PATH, 'isis.sh'), 'stats', 'from='+file ], stdout=PIPE)
    stats = p.communicate()[0]
    tokens = stats.split('\n')
    minimum = 0.0
    maximum = 0.0
    for t in tokens:
        subtokens = t.split('=')
        if (len(subtokens) > 1):
            param = subtokens[0].strip()
            if (param == "Minimum"):
                minimum = float(subtokens[1].strip())
            if (param == "Maximum"):
                maximum = float(subtokens[1].strip())

    print "min: %f\tmax: %f" % (minimum, maximum)
    return (minimum, maximum)

def stretch2int8(infile, outfile):
     # stretch from=/home/ted/e1501055.cub to=/home/ted/e1501055_8bit.cub+8bit+0:254 pairs="0.092769:1 0.183480:254" null=0 lis=1 lrs=1 his=255 hrs=255 
    (minval, maxval) = getminmax(infile)
    args = (
        'stretch',
        'from='+infile,
        'to='+outfile+'+8bit+1:255',
        'pairs=%f:1 %f:255' % (minval, maxval),
        'null=0',
        'lis=1',
        'lrs=1',
        'his=255',
        'hrs=255',
    )
    try:
        retcode = isis_run(args, message="Converting to int8: %s --> %s" % (infile,outfile))
    finally:
        unlink_if_exists(infile)

def image2plate(imagefile, platefile):
    cmd = [IMAGE2PLATE,]
    if options.transaction_id:
        cmd += ('-t', str(options.transaction_id))
    cmd += ('--file-type auto -o', platefile, imagefile)
    cmd = ' '.join(cmd)

    exit_status = execute(cmd, pretend=not options.write_to_plate)
    if exit_status != 0:
        raise Exception("image2plate failed!")



import urlparse
import urllib
def ctx2plate(ctxurl, platefile):
    '''
    Process a CTX image from its raw form into a platefile.
    Download the file first, if a remote url is given.
    '''
    
    parsed = urlparse.urlparse(ctxurl, 'http')
    using_tmpfile = False
    if parsed.netloc:
        # download the file
        (ctxfile, headers) = urllib.urlretrieve(parsed.geturl())
        if ctxfile != ctxurl:
            using_tmpfile = True # this file will be deleted at the end of the script
    else:
        ctxfile = parsed.path
    try:
        assert os.path.exists(ctxfile)
        print "Commencing ctx2plate: %s --> %s" % (ctxfile, platefile)

        # intermediate filenames
        imgname = os.path.splitext(os.path.basename(ctxfile))[0]
        original_cube =  os.path.join(options.tmpdir, imgname+'.cub')
        nonull_cube = os.path.join(options.tmpdir, 'nonull_'+imgname+'.cub')
        calibrated_cube = os.path.join(options.tmpdir, 'calibrated_'+imgname+'.cub')
        lineeq_cube = os.path.join(options.tmpdir, 'lineeq_'+imgname+'.cub')
        projected_cube = os.path.join(options.tmpdir, 'projected_'+imgname+'.cub')
        stretched_cube = os.path.join(options.tmpdir, 'stretched_'+imgname+'.cub')

        # ISIS INGESTION
        ctx2isis(ctxfile, original_cube)
        spiceinit(os.path.join(options.tmpdir, original_cube))

        # Reject useless images
        if get_percent_valid(original_cube) < VALIDITY_THRESHOLD:
            raise Exception("Too many invalid pixels in %s" % original_cube)
        if EMISSION_ANGLE_THRESHOLD > 0:
            fail_high_emission_angles(original_cube, emission_angle_threshold=EMISSION_ANGLE_THRESHOLD)

        # PREPROCESS
		# Let's leave null2lrs out, CTX images are 16 bit, so can't just stretch to 255 at this stage.
        null2lrs(original_cube, nonull_cube)
        calibrate(nonull_cube, calibrated_cube)
        lineeq(calibrated_cube, lineeq_cube)
        map_project(lineeq_cube, projected_cube)
        stretch2int8(projected_cube, stretched_cube)

        #Delete original tmpfile, if it exists
        if using_tmpfile:
            unlink_if_exists(ctxfile)
    except:
        traceback.print_exc()
        return 129 # special status code 129 indicates non-blocking failure
    
    # MipMap & add to platefile
    if options.write_to_plate and not options.dry_run:
        try:
            image2plate(stretched_cube, platefile)
        finally:
            unlink_if_exists(stretched_cube)
    else:
        print "DRY RUN.  %s would be added to %s" % (stretched_cube, platefile)
    print "ctx2plate completed."
    return 0

def main():
    global options
    from optparse import OptionParser
    usage = "%prog sourceimage.img platefile [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('--tmpdir', action='store', dest='tmpdir', help="Where to write intermediate images (default: %s)" % DEFAULT_TMP_DIR)
    parser.add_option('-t', '--transaction-id', action='store', dest='transaction_id', type='int')
    parser.add_option('--preserve', '-p', dest='delete_files', action='store_false', help="Don't delete the intermediate files.")
    parser.add_option('--noplate', dest='write_to_plate', action='store_false', help="Like --dry-run, except run everything but image2plate")
    parser.add_option('--dry-run', dest='dry_run', action='store_true', help='Print the commands to be run without actually running them') # NOTE: --dry-run will always throw errors, because we can't use the isis tools to pull values and stats from intermediate files that don't exist!
    parser.add_option('-v', '--verbose', dest='verbose', action='store_true', help='More output!')
    parser.set_defaults(tmpdir=DEFAULT_TMP_DIR, transaction_id=None, delete_files=True, write_to_plate=True, dry_run=False, verbose=False)
    (options, args) = parser.parse_args()
    if len(args) != 2:
        parser.print_help()
        sys.exit(1)
    else:
        (input_url, platefile) = args

    retcode = ctx2plate(input_url, platefile)    
    sys.exit(retcode)

if __name__ == '__main__':
    main()
