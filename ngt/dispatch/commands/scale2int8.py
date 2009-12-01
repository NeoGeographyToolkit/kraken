#!/usr/bin/env python
import sys, os
from subprocess import Popen, PIPE

### Global Params
# COMMAND_PATH = os.path.dirname(__file__) if os.path.dirname(__file__)).strip() else '.' # because this file lives in the command path
DEFAULT_OUTPATH = "out/"

if os.path.dirname(__file__).strip():
    COMMAND_PATH = os.path.abspath(os.path.dirname(__file__))
else:
    COMMAND_PATH = os.path.abspath(os.getcwd())
print "command path is %s" % COMMAND_PATH

def isis_run(message, args):
    print message
    #print os.path.join(COMMAND_PATH, 'isis.sh')
    os.chdir('/tmp/')
    p = Popen([os.path.join(COMMAND_PATH, 'isis.sh')]+list(args), shell=False)
    return p.wait()
    
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
    
def haserrors(cubefile):
    p = Popen([ os.path.join(COMMAND_PATH, 'isis.sh'), 'getkey', 'from='+cubefile, 'key=DataQualityDesc', 'recursive=true'], stdout=PIPE)
    outp = p.communicate()[0]
    lines = outp.split('\n')
    result = [l.strip() for l in lines]
    if 'OK' in result:
        print "No errors in cube."
        return False
    elif 'ERRORS' in result:
        print "Errors found in cube.  This on will be failed and skipped."
        return True
    else:
        raise Exception, "Unexpected result from getkey."
    
def stretch(infile, outfile, minval, maxval):
    # stretch from=/home/ted/e1501055.cub to=/home/ted/e1501055_8bit.cub+8bit+0:254 pairs="0.092769:1 0.183480:254" null=0 lis=1 lrs=1 his=255 hrs=255 
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
    return isis_run("Converting to int8: %s --> %s" % (infile,outfile), args)

def convert(infile, outfile):
    minval, maxval = getminmax(infile)
    
    retcode = stretch(infile, outfile, minval, maxval)
    #sys.exit(retcode)
    return retcode

if __name__ == '__main__':
    from optparse import OptionParser
    usage = '''USAGE: scale2int8.py sourceimage.cub  destination.cub  '''
    parser = OptionParser(usage=usage)
    (options, args) = parser.parse_args()
    if len(args) < 1:
        parser.print_help()
        sys.exit(1)
    elif len(args) > 1:
        outfile = args[1]
    else:
        outfile = DEFAULT_OUTPATH + os.path.splitext(os.path.basename(args[0]))[0] + "_8bit.cub"
    infile = args[0]
    if haserrors(infile):
        sys.exit(6)
    if not os.path.exists(os.path.dirname(outfile)):
        os.makedirs(os.path.dirname(outfile))
    retcode = convert(infile, outfile)
    sys.exit(retcode)
