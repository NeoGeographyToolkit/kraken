#!/usr/bin/env python2.6
import sys, os
import optparse

dirname = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(dirname, '../..'))
sys.path.insert(0, os.path.join(dirname, '..'))

from django.core.management import setup_environ
from ngt import settings
setup_environ(settings)

from django.db import transaction
from ngt.jobs.models import Job, JobSet

def make_jobs(jobset, commandfile):
    print "Creating Jobs..."
    i = 0
    for line in commandfile:
        line = line.strip()
        args = line.split(' ')
        job = Job()
        job.command = args.pop(0)
        job.arguments = args
        job.jobset = jobset
        job.save()
        i += 1
        sys.stderr.write("\r%d" % i)
        sys.stderr.flush()
    sys.stderr.write("\n")
        

def make_jobset(commandfile_name):
    commandfile = open(commandfile_name, 'r')
    jobset_name = commandfile.next()
    print "Creating JobSet."
    jobset = JobSet()
    jobset.name = jobset_name
    jobset.command = options.command
    jobset.priority = options.priority
    jobset.save()
    make_jobs(jobset, commandfile)
    print "JobSet %d created." % jobset.id
    if options.activate:
        print "Activating!"
        jobset.active = True
        jobset.save()
    else:
        print "Not activated."

if __name__ == '__main__':
    global options
    parser = optparse.OptionParser()
    parser.add_option('-a', '--activate', action='store_true', dest='activate', help='Activate the jobset after creation')
    parser.add_option('-p', '--priority', action='store', dest='priority', type='int', help='Dispatch priority for the new JobSet. (default: 3)')
    parser.set_defaults(activate=False, priority=3, command='generic')
    parser.set_usage("""
        %prog commandfile
        The first line of commandfile should be the name of the new JobSet.
        The rest of commandfile should be a list of commands to add to the JobSet.
    """)
    (options, args) = parser.parse_args()
    if len(args) < 1:
        parser.print_help()
        sys.exit(1)
    commandfile_name = args[0]
    make_jobset(commandfile_name)
