#!/usr/bin/env python

import glob, sys, os.path
import optparse

sys.path.insert(0, '../..') # or whatever the relative path to the ngt directory is...
from django.core.management import setup_environ
from ngt import settings
setup_environ(settings)

from ngt.jobs.models import Job, JobSet
from django.db import transaction

DEFAULT_PLATEFILE = 'pf://ptk/apollo_15_drg.ptk'
DEFAULT_ITERATIONS = 100
DEFAULT_ALBEDO_JOBS = 80
DEFAULT_TIME_ESTIMATE_JOBS = 80
ITERATION_MAX_LEVEL = 10
POLISH_MAX_LEVEL = 13

if len(sys.argv) > 1:
    storagedir = sys.argv[1]
else:
   print "Usage: %s drg_dirname" % __file__
   sys.exit(1)

def make_job_simple(command, args, jobset=None):
    """ Create a Job and add it to a JobSet, if one is supplied. """
    print "create job: %s %s" % (command, " ".join(args))
    job = Job(command=command)
    job.arguments = args
    if jobset:  
        job.jobset = jobset
    job.save()
    return job



def create_albedo_jobs(jobset, n_jobs, max_level, dependencies=[]):
        albedo_jobs = []
        for i in range(options.albedo_jobs):
            args = "-l %d -j %d -n %d" % (max_level, i, n_jobs)
            args = args.split(' ')
            job = make_job_simple('phoitalbedo', args, jobset)
            for dep in dependencies:
                job.dependencies.add(dep)
            albedo_jobs.append(job)
        return albedo_jobs

def create_time_estimate_jobs(jobset, max_level, n_jobs, platefile, dependencies=[]):
        time_job_list = []
        for i in range(n_jobs):
            args = "-l %d -j %d -n %d" % (max_level, i, n_jobs)
            args = args.split(" ")
            job = make_job_simple("phoittime", args, platefile)
            for dep in dependencies:
                job.dependencies.add(dep)
            time_job_list.append(job)
        return time_job_list

def create_iteration_jobs(options, jobset, initial_dependencies=[]):
    time_job_list = []
    for iteration in range(options.iterations):
        print "Generating jobs for iteration %d" % iteration
        
        albedo_jobs = create_albedo_jobs(jobset, options.albedo_jobs, options.iteration_max_level, dependencies=time_job_list)

        """
        mipmap_job = make_job_simple('pho_mipmap', [options.platefile,], jobset)
        last_mipmap = mipmap_job
        for j in albedo_jobs:
            mipmap_job.dependencies.add(j)
        """
        
        time_job_list = create_time_estimate_jobs(jobset, options.iteration_max_level, options.time_jobs, options.platefile, dependencies=albedo_jobs)

    return time_job_list

def create_mipmap_job(platefile, dependencies=[]):
    job = make_job_simple('pho_mipmap', [platefile,], jobset)
    for dep in dependencies:
        job.dependencies.add(dep)
    return job
         
@transaction.commit_on_success
def phosolve(options):
    jobset = JobSet(name="Apollo 15 Metric Camera Photometry")
    jobset.save()

    last_time_job_list = create_iteration_jobs(options, jobset)
    polish_albedo_jobs = create_albedo_jobs(jobset, options.albedo_jobs, options.polish_max_level, dependencies=last_time_job_list)
    create_mipmap_job(options.platefile, dependencies=polish_albedo_jobs)


if __name__ == '__main__':
    parser = optparse.OptionParser(usage="usage: %prog [options]")
    parser.add_option('-i', '--iterations', dest='iterations', default=DEFAULT_ITERATIONS, type='int')
    parser.add_option('-a', '--albedo-jobs', dest='albedo_jobs', default=DEFAULT_ALBEDO_JOBS), type='int'
    parser.add_option('-t', '--time-jobs', dest='time_jobs', default=DEFAULT_TIME_ESTIMATE_JOBS)i, type='int'
    parser.add_option('-p', '--platefile', dest='platefile', default=DEFAULT_PLATEFILE)
    parser.set_defaults(iteration_max_level=ITERATION_MAX_LEVEL, polish_max_level=POLISH_MAX_LEVEL)
    (options, args) = parser.parse_args()

    phosolve(options)
