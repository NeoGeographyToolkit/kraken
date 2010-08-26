#!/usr/bin/env python
from subprocess import Popen
import os.path, sys, os
import optparse

VW_BIN_DIR = '/big/software/visionworkbench/bin/'
STEREO_BIN_DIR = '/big/software/stereopipeline/bin'
DEFAULT_PLATEFILE = 'pf://ptk/apollo15_phodrg.ptk'

def pho_cmd(command):
    return os.path.join(STEREO_BIN_DIR, command)

if __name__ == '__main__':
    
    parser = optparse.OptionParser()
    parser.add_option("--platefile", dest="platefile", default=DEFAULT_PLATEFILE)
    (options, args) = parser.parse_args()

    assert len(args) > 0
    drg_file = args[0]
    grass_file = os.path.splitext(drg_file)[0] + '_grass.tif'
    shadow_file = os.path.splitext(grass_file)[0] + '_shdw.tif'
    
    if os.path.split(drg_file)[0]:
        os.chdir(os.path.split(drg_file)[0])

    print "%s --> %s" % (drg_file, options.platefile)
    print "via ", grass_file
    print "via ", shadow_file

    try:
        print "grassfire alpha...",
        args = (os.path.join(VW_BIN_DIR,'grassfirealpha'), drg_file)
        p = Popen(args)
        assert p.wait() == 0
        print "Done."
    except:
        sys.exit("grassfirealpha failed.")

    try:
        print "Shadow mask...",
        p = Popen((pho_cmd('shadow_mask'), '--feather', grass_file))
        assert p.wait() == 0
        print "Done."
    except:
        sys.exit("shadow_mask failed")
    finally:
        os.unlink(grass_file)

"""
    try:
        print "phodrg2plate...",
        p = Popen((pho_cmd('phodrg2plate'), options.platefile, shadow_file))
        assert p.wait() == 0
        print "Done."
    except:
        sys.exit("phodrg2plate failed.")
    finally:
        os.unlink(shadow_file)
"""
print "Done"
