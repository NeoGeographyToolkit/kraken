#!/usr/bin/env python
"""
This is a one-off script for debugging,
Preserved here for posterity.
It writes out the image2plate and snapshot commands invoked by a particular pair of jobsets.
"""

from ngt.jobs.models import *

image2plate_jobset_id = 232
snapshot_jobset_id = 233

def reconstruct_image2plates(jobset, outfile):
    print "Writing image2plate jobs to %s..." % outfile.name
    for j in jobset.jobs.order_by('time_started'):
        transaction_id = j.transaction_id
        platefile = j.arguments[1]
        imagefile = "stretched_" + os.path.split(j.arguments[0])[-1].replace('IMG','CUB')
        outfile.write( str(j.time_started) + "\t")
        outfile.write( str(j.time_ended) + "\t")
        outfile.write("image2plate -m equi -t %d --filetype auto %s %s\n" % (transaction_id, platefile, imagefile))
    print "done."

def reconstruct_snapshots(jobset, outfile):
    print "Writing snapshot jobs to %s..." % outfile.name
    jobs = jobset.jobs.order_by('time_started')
    for j in jobs:
        assert j.status == 'complete'
        outfile.write( str(j.time_started) + "\t")
        outfile.write( str(j.time_ended) + "\t")
        invocation = j.command_string.replace('start_','').replace('end_','')
        outfile.write(invocation+"\n")
    print "done."

def reconstruct():
    i2p_out = 'image2plate_jobs.txt'
    snapshot_out = 'snapshot_jobs.txt'

    i2p_jobset = JobSet.get(image2plate_jobset_id)
    with open(i2p_out, 'w') as outfile:
        reconstruct_image2plates(i2p_jobset, outfile)

    snapshot_jobset = JobSet.get(snapshot_jobset_id)
    with open(snapshot_out, 'w') as outfile:
        reconstruct_snapshots(snapshot_jobset, outfile)
