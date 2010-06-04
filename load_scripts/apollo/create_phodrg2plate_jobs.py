#!/usr/bin/env python

import glob, sys, os.path
import optparse

sys.path.insert(0, '../..') # or whatever the relative path to the ngt directory is...
from django.core.management import setup_environ
from ngt import settings
setup_environ(settings)

from ngt.jobs.models import Job, JobSet
from django.db import transaction

DEFAULT_PLATEFILE = 'pf://wwt10one/ptk/apollo_15_drg.ptk'

@transaction.commit_on_success
def main(options, imagedir):

    shdw_files = glob.glob(os.path.join(imagedir, '*shdw.tif'))
    if len(shdw_files) < 1:
        exit("No shadow files found")

    else:
        print "Found %d shadow files." % len(shdw_files)

    jobset = JobSet(name="Apollo 15 phodrg2plate")
    jobset.save()
    for file in shdw_files: 
        job = Job(command="phodrg2plate")
        job.arguments = [DEFAULT_PLATEFILE, os.path.join(imagedir, file)]
        job.jobset = jobset
        job.save()
    
    print "Created %s" % str(jobset)
    if options.activate:
        jobset.active = True
        jobset.save()
        print "Activated."
        
if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('--platefile', '-p', dest='platefile')
    parser.add_option('--activate', dest='activate', action='store_true')
    parser.set_defaults(platefile=DEFAULT_PLATEFILE, activate=False)
    (options, args) = parser.parse_args()
    if len(args) < 1:
        sys.exit("USAGE: create_phodrg2plate_jobs imagedir")
    else:
        imagedir = args[0]
    
    main(options, imagedir)
