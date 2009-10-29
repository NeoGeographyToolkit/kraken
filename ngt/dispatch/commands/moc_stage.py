import sys, os
from subprocess import Popen

### Global Params
COMMAND_PATH = (os.path.dirname(__file__)) if (os.path.dirname(__file__)).strip() else '.'# because this file lives in the command path
print "command path is %s" % COMMAND_PATH

def isis_run(message, args):
    print message
    print os.path.join(COMMAND_PATH, 'isis.sh')
    p = Popen([os.path.join(COMMAND_PATH, 'isis.sh')]+list(args))
    p.wait()
    print "Done."


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

def stage_image(image_path, output_dir="/big/assets/moc/"):
   basename = os.path.splitext(os.path.basename(image_path))[0]
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
   assert os.path.exists(tmpfilename[1:])
   os.remove(tmpfilename)

   print "YAY!!!"
   sys.exit(0)

if __name__ == '__main__':
    usage = '''USAGE: stage_moc.py sourceimage [outputpath] '''
    if len(sys.argv) < 2:
        print usage
        sys.exit(1)
    elif len(sys.argv) > 2:
        stage_image(sys.argv[1], output_dir=sys.argv[2])
    else:
        stage_image(sys.argv[1])
