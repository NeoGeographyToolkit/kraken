#!/usr/bin/env python
import sys, os
from subprocess import Popen, PIPE

### Global Params
# COMMAND_PATH = os.path.dirname(__file__) if os.path.dirname(__file__)).strip() else '.' # because this file lives in the command path
DEFAULT_OUTPATH = "out/"

if os.path.dirname(__file__).strip():
    COMMAND_PATH = os.path.dirname(__file__)
else:
    COMMAND_PATH = '.' # because this file lives in the command path
print "command path is %s" % COMMAND_PATH

def isis_run(message, args):
    print message
    #print os.path.join(COMMAND_PATH, 'isis.sh')
    p = Popen([os.path.join(COMMAND_PATH, 'isis.sh')]+list(args))
    return p.wait()
    
def getminmax(file):
    stats = isis_run("Computing stats for %s" % file, (stats, 'from='+file))
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
    
def stretch(infile, outfile, minval, maxval):
    # stretch from=test.cub to=out.cub+8bit pairs=\"in_min:1 in_max:254\" null=0 lis=1 lrs=1 his=255 hrs=255
    args = (
        'stretch',
        'from='+infile,
        'to='+outfile+'+8bit',
        'pairs=\"%f:1 %f:254\"' % (minval, maxval),
        'null=0',
        'lis=1',
        'lrs=1'
        'his=255',
        'hrs=255',
    )
    return isis_run("Converting to int8: %s --> %s" % (infile,outfile), args)


if __name__ == '__main__':
    from optparse import OptionParser
    usage = '''USAGE: scale2int8.py sourceimage.cub  destination.cub  '''
    parser = OptionParser(usage=usage)
    parser.add_option("-m", "--map", default='Sinusoidal', dest='map_projection', help='ISIS name of a map projection to use.')
    (options, args) = parser.parse_args()
    if len(args) < 1:
        parser.print_help()
        sys.exit(1)
    elif len(args) > 1:
        outfile = args[1]
    else:
        outfile = DEFAULT_OUTPATH + os.path.splitext(os.path.basename(args[0]))[0] + ".cub"
    infile = args[0]
    minval, maxval = getminmax(infile)
    
    retcode = stretch(infile, outfile, minval, maxval)
    sys.exit(retcode)