#!/usr/bin/env python
import sys, os
from subprocess import Popen

### Global Params
# COMMAND_PATH = os.path.dirname(__file__) if os.path.dirname(__file__)).strip() else '.' # because this file lives in the command path
DEFAULT_OUTPATH = "out/"

if os.path.dirname(__file__).strip():
    COMMAND_PATH = os.path.abspath(os.path.dirname(__file__))
else:
    COMMAND_PATH = os.path.abspath(os.getcwd()) # because this file lives in the command path
print "command path is %s" % COMMAND_PATH

def isis_run(message, args):
    print message
    #print os.path.join(COMMAND_PATH, 'isis.sh')
    p = Popen([os.path.join(COMMAND_PATH, 'isis.sh')]+list(args))
    return p.wait()

mapfiles = {
    'PolarStereographic': 'polarstereographic.map',
    'Sinusoidal': 'sinusoidal.map',
}
def mocproc(input_file, output_file, map=False):
    msg = "%s --> %s" % (input_file, output_file)
    output_dir = os.path.split(output_file)[0]
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    args = ('mocproc', 'from='+input_file, 'to='+output_file)
    if map and map in mapfiles:
        mapfile = os.path.join(COMMAND_PATH,mapfiles[map])
        args = args + ("map="+mapfile,)
    return isis_run(msg, args)

if __name__ == '__main__':
    from optparse import OptionParser
    usage = '''USAGE: stage_moc.py sourceimage.img destination.cub  '''
    parser = OptionParser(usage=usage)
    parser.add_option("-m", "--map", default='Sinusoidal', dest='map_projection', help='ISIS name of a map projection to use.')
    (options, args) = parser.parse_args()
    os.chdir('/tmp/')
    if len(args) < 1:
        parser.print_help()
        sys.exit(1)
    elif len(args) > 1:
        #stage_image(sys.argv[1], output_dir=sys.argv[2])
        outfile = args[1]
    else:
        outfile = DEFAULT_OUTPATH + os.path.splitext(os.path.basename(args[0]))[0] + ".cub"
        #stage_image(sys.argv[1], outfile)
    retcode = mocproc(sys.argv[1], outfile, map=options.map_projection)
    sys.exit(retcode)





"""
def make_cube(source_img_path, dest_cube_path):
    assert os.path.exists(source_img_path)
    msg =  "Creating new CUB at %s" % dest_cube_path
    isis_run(msg, ('moc2isis', 'from='+source_img_path, 'to='+dest_cube_path) )

def spiceinit(cube):
    msg = "Adding spice headers..."
    isis_run(msg, ('spiceinit', 'from='+cube))

def cam2map(incube, outcube):
    msg = "Projecting to %s" % outcube
    isis_run(msg, ('cam2map', 'from='+incube, 'to='+outcube))

def stage_image(image_path, output_dir="/big/assets/moc/", flat=True):
   basename = os.path.splitext(os.path.basename(image_path))[0]
   if flat:
    dest_dir = output_dir
   else:
    dest_dir = os.path.join(output_dir, basename[:5])
   if not os.path.exists(dest_dir):
       os.makedirs(dest_dir)
   tmpfilename = "_%s.cub" % basename
   tmpfilename = os.path.join(dest_dir, tmpfilename)
   make_cube(image_path, tmpfilename)
   assert os.path.exists(tmpfilename)

   spiceinit(tmpfilename)
   tf = open(tmpfilename, 'r')
   header = tf.read(2*1024)
   tf.close()
   assert 'NaifFrameCode' in header
   del header
   del tf

   projfile = tmpfilename.replace('_','')
   cam2map(tmpfilename, projfile)
   assert os.path.exists(projfile)
   os.remove(tmpfilename)

   print "Projected map saved to %s" % projfile
   #sys.exit(0) 
"""
