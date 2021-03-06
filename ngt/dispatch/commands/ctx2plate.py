#!/usr/bin/env python
import sys, os, os.path
import subprocess
from subprocess import Popen, PIPE
import multiprocessing
import resource
import shlex
import traceback
import urlparse
import urllib
import json

DEFAULT_TMP_DIR = '/scratch/tmp'
DEFAULT_CACHE_DIR = '/big/scratch/ctxcache'
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
VW_PATH = '/big/local/visionworkbench/bin'
IMAGE2PLATE = 'image2plate'
if os.path.exists(VW_PATH): # We're on the cluster!
    IMAGE2PLATE = os.path.join(VW_PATH, IMAGE2PLATE)

def isis_run(args, message=None, pretend=None, display=True):
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

def fail_poor_data_quality(file):
    print "Checking data quality..."
    p = Popen(['head', '-n', '46', file]) # Assuming all attached PDS labels are the same line length, which they ought to be.
    lines = p.communicate()[0].split('\n')
    for line in lines:
        tokens = split('=')
        if tokens[0].strip() == 'DATA_QUALITY_DESC':
            if tokens[1].strip() == 'OK':
                return 0
            else:
                raise Exception("Failed data quality check: %s" % tokens[1].strip())
    else:
        raise Exception("Data quality header not found.")

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

def cubenorm(incube, outcube):
    try:
        isis_run(('cubenorm', 'from='+incube, 'to='+outcube), message="Running cubenorm.")
    finally:
        unlink_if_exists(incube)

def bandnorm(incube, outcube):
    try:
        isis_run(('bandnorm', 'from='+incube, 'to='+outcube), message="Running bandnorm.")
    finally:
        unlink_if_exists(incube)

def histeq(incube, outcube):
    try:
        isis_run(('histeq', 'from='+incube, 'to='+outcube), message="Running histeq.")
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
           

#def getminmax(file):
#    p = Popen([ os.path.join(COMMAND_PATH, 'isis.sh'), 'stats', 'from='+file ], stdout=PIPE)
#    stats = p.communicate()[0]
#    tokens = stats.split('\n')
#    minimum = 0.0
#    maximum = 0.0
#    for t in tokens:
#        subtokens = t.split('=')
#        if (len(subtokens) > 1):
#            param = subtokens[0].strip()
#            if (param == "Minimum"):
#                minimum = float(subtokens[1].strip())
#            if (param == "Maximum"):
#                maximum = float(subtokens[1].strip())
#
#    print "min: %f\tmax: %f" % (minimum, maximum)
#    return (minimum, maximum)

def get_stats(incube):
    p = Popen([ os.path.join(COMMAND_PATH, 'isis.sh'), 'stats', 'from='+incube ], stdout=PIPE)
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
            if (param == "Average"):
                mean = float(subtokens[1].strip())
            if (param == "StandardDeviation"):
                stdev = float(subtokens[1].strip())
    return (minimum, maximum, mean, stdev)


def stretch2int8(infile, outfile, standard_deviations=0.0, use_percentages=False):
    # stretch from=/home/ted/e1501055.cub to=/home/ted/e1501055_8bit.cub+8bit+0:254 pairs="0.092769:1 0.183480:254" null=0 lis=1 lrs=1 his=255 hrs=255

    if use_percentages:
        pairs = "0.27:1 50:128 99.73:255"  # an alternative way to cut the tails on the histogram should work faster since it doesn't depend on the stats function.
    else:
        (minval, maxval, mean, stdev) = get_stats(infile)
        if (standard_deviations > 0.1 ):
            if (mean-standard_deviations*stdev) > minval:
                minval = mean-standard_deviations*stdev
            if (mean+standard_deviations*stdev) < maxval:
                maxval = mean+standard_deviations*stdev
        pairs = "%f:1 %f:255" % (minval, maxval)

    args = (
        'stretch',
        'from='+infile,
        'to='+outfile+'+8bit+1:255',
        'pairs='+pairs,
        'null=0',
        'lis=1',
        'lrs=1',
        'his=255',
        'hrs=255',
        )
    if use_percentages:
        args += ('usepercentages=true',)
    try:
        retcode = isis_run(args, message="Converting to int8: %s --> %s" % (infile,outfile))
    finally:
        unlink_if_exists(infile)


def image2plate(imagefile, platefile):
    cmd = [IMAGE2PLATE,]
    cmd += ('-m', 'equi') # equirectangular projection for GE
    if options.transaction_id:
        cmd += ('-t', str(options.transaction_id))
    cmd += ('--file-type auto -o', platefile, imagefile)
    cmd = ' '.join(cmd)
    print cmd

    parent_conn, child_conn = multiprocessing.Pipe()
    pretend = not options.write_to_plate
    p = multiprocessing.Process(target=_image2plate, args=(child_conn, cmd, pretend))
    p.start()

    exit_status, rusage = parent_conn.recv()
    p.join()

    print "BEGIN_IMAGE2PLATE_RUSAGE",
    print json.dumps(tuple(rusage)),
    print "END_IMAGE2PLATE_RUSAGE"

    if exit_status != 0:
        raise Exception("image2plate failed!")

def _image2plate(conn, cmd, pretend):
    exit_status = execute(cmd, pretend=not options.write_to_plate)
    rusage = resource.getrusage(resource.RUSAGE_CHILDREN)
    conn.send((exit_status, rusage))
    sys.stdout.flush()


def reduce(infile, outfile, percent):
    scalefactor_inverse = 1/(percent/100.00)
    scalestr = "%1.6f" % scalefactor_inverse
    args = (
        'reduce',
        'from='+infile,
        'to='+outfile,
        'reduction_type=SCALE',
        'sscale='+scalestr,
        'lscale='+scalestr,
    )
    try:
        retcode = isis_run(args, message="Scaling to %d percent." % percent)

    finally:
        unlink_if_exists(infile)


def dl_report(nblocks, blocksize, totalsize):
    sys.stdout.write("\r%d of %d blocks transferred" % (nblocks, totalsize / blocksize))
    sys.stdout.flush()

def download(parsed_url, destfile, retries=2):
    print "Downloading: "+parsed_url.geturl()
    try:
        (ctxfile, headers) = urllib.urlretrieve(parsed_url.geturl(), destfile, reporthook=dl_report)
    except:
        if retries < 1:
            raise
        return download(parsed_url, destfile, retries=retries - 1)
    print "\n"
    return ctxfile
    

def ctx2plate(ctxurl, platefile):
    '''
    Process a CTX image from its raw form into a platefile.
    Download the file first, if a remote url is given.
    '''
    
    parsed_url= urlparse.urlparse(ctxurl, 'http')
    destfile = os.path.join(options.tmpdir, parsed_url.path.split('/')[-1])
    # intermediate filenames
    imgname = os.path.splitext(os.path.basename(destfile))[0]
    original_cube =  os.path.join(options.tmpdir, imgname+'.cub')
    reduced_cube = os.path.join(options.tmpdir, 'reduced_'+imgname+'.cub')
    nonull_cube = os.path.join(options.tmpdir, 'nonull_'+imgname+'.cub')
    calibrated_cube = os.path.join(options.tmpdir, 'calibrated_'+imgname+'.cub')
    norm_cube = os.path.join(options.tmpdir, 'norm_'+imgname+'.cub')
    bandnorm_cube = os.path.join(options.tmpdir, 'bandnorm_'+imgname+'.cub')
    histeq_cube = os.path.join(options.tmpdir, 'histeq_'+imgname+'.cub')
    projected_cube = os.path.join(options.tmpdir, 'projected_'+imgname+'.cub')
    stretched_cube = os.path.join(options.cachedir, 'stretched_'+imgname+'.cub') ### NOTE: This file gets saved to a different location

    if options.caching and os.path.exists(stretched_cube):
        # Shortcut preprocessing and add the stretched cube to plate
        print "File %s already exists!  Skipping preprocessing and running img2plate." % stretched_cube
        if options.write_to_plate and not options.dry_run:
            image2plate(stretched_cube, platefile)
        else:
            print "DRY RUN.  %s would be added to %s" % (stretched_cube, platefile)
        print "ctx2plate completed."
        return 0

    using_tmpfile = False
    if parsed_url.netloc:
        # download the file
        ctxfile = download(parsed_url, destfile)
        if ctxfile != ctxurl: # implies we were given a remote URL rather than a local file path
            using_tmpfile = True # this file will be deleted at the end of the script
    else:
        ctxfile = parsed_url.path
    try:
        assert os.path.exists(ctxfile)
        print "Commencing ctx2plate: %s --> %s" % (ctxfile, platefile)
        
        # Filter on data quality flag
        # This is disabled because the production create_mosaic_jobs script is doing this now.
        #fail_poor_data_quality(ctxfile)

        # ISIS INGESTION
        ctx2isis(ctxfile, original_cube)
        spiceinit(os.path.join(options.tmpdir, original_cube))


        # Reject useless images
        if get_percent_valid(original_cube) < VALIDITY_THRESHOLD:
            raise Exception("Too many invalid pixels in %s" % original_cube)
        if EMISSION_ANGLE_THRESHOLD > 0:
            fail_high_emission_angles(original_cube, emission_angle_threshold=EMISSION_ANGLE_THRESHOLD)
        ###
        # PREPROCESS
        ###

		# Let's leave null2lrs out, CTX images are 16 bit, so can't just stretch to 255 at this stage.
        null2lrs(original_cube, nonull_cube)
        calibrate(nonull_cube, calibrated_cube)

        # DOWNSAMPLE
        if options.downsample < 100:
            reduce(calibrated_cube, reduced_cube, options.downsample)
            unlink_if_exists(calibrated_cube)
            working_cube = reduced_cube
        else:
            working_cube = calibrated_cube # skip downsampling

        cubenorm(working_cube, norm_cube)
        working_cube = norm_cube

        if options.bandnorm:
            bandnorm(working_cube, bandnorm_cube)
            working_cube = bandnorm_cube

        if options.histeq:
            histeq(norm_cube, histeq_cube)
            working_cube = histeq_cube

        map_project(working_cube, projected_cube)

        stretch2int8(projected_cube, stretched_cube, standard_deviations=options.clipping, use_percentages=options.use_percentages)

        #Delete original tmpfile, if it exists
        if using_tmpfile:
            unlink_if_exists(ctxfile)
    except:
        traceback.print_exc()
        return 129 # special status code 129 indicates non-blocking failure
    
    # MipMap & add to platefile
    if options.write_to_plate and not options.dry_run:
        image2plate(stretched_cube, platefile)
        unlink_if_exists(stretched_cube)
    else:
        print "DRY RUN.  %s would be added to %s" % (stretched_cube, platefile)
    print "ctx2plate completed."
    return 0

def main():
    global options
    from optparse import OptionParser
    usage = "%prog source_url platefile [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('--tmpdir', action='store', dest='tmpdir', help="Where to write intermediate images (default: %s)" % DEFAULT_TMP_DIR)
    parser.add_option('--cachedir', action='store', dest='cachedir', help="Where to write final (pre-plate) images for caching(default: %s)" % DEFAULT_CACHE_DIR)
    parser.add_option('--nocache', action='store_false', dest='caching', help='Disable caching (force preprocessing even if the output cube exists).')
    parser.add_option('-t', '--transaction-id', action='store', dest='transaction_id', type='int')
    parser.add_option('--preserve', '-p', dest='delete_files', action='store_false', help="Don't delete the intermediate files.")
    parser.add_option('--noplate', dest='write_to_plate', action='store_false', help="Like --dry-run, except run everything but image2plate")
    parser.add_option('--dry-run', dest='dry_run', action='store_true', help='Print the commands to be run without actually running them') # NOTE: --dry-run will always throw errors, because we can't use the isis tools to pull values and stats from intermediate files that don't exist!
    parser.add_option('--downsample', dest='downsample', action='store', type='float', help="Percentage to downsample (as float)")
    parser.add_option('--histeq', dest='histeq', action='store_true', help="Apply histogram equalization", default=False)
    parser.add_option('-c', '--clipping', dest='clipping', action='store', type='float', help="Clip intensity values beyond N standard deviations from the mean. 0 disables. (Default 3)")
    parser.add_option('--bandnorm', dest='bandnorm', action='store_true', help="Apply ISIS bandnorm tool.")
    parser.add_option('--percentages', dest='use_percentages', action='store_true', help="Use percentages instead of values for the stretch step (overrides clipping setting)")
    parser.add_option('-v', '--verbose', dest='verbose', action='store_true', help='More output!')
    parser.set_defaults(
        caching=True, 
        cachedir=DEFAULT_CACHE_DIR, 
        tmpdir=DEFAULT_TMP_DIR, 
        transaction_id=None, 
        delete_files=True, 
        write_to_plate=True, 
        dry_run=False, 
        verbose=False, 
        bandnorm=False, 
        histeq=False,
        downsample=100, 
        clipping=3.0,
        use_percentages=False,
    )
    (options, args) = parser.parse_args()
    if options.tmpdir != DEFAULT_TMP_DIR and options.cachedir == DEFAULT_CACHE_DIR:
        options.cachedir = options.tmpdir
    if len(args) != 2:
        parser.print_help()
        sys.exit(1)

    (input_url, platefile) = args

    retcode = ctx2plate(input_url, platefile)    
    sys.exit(retcode)

if __name__ == '__main__':
    main()
