#!/usr/bin/env python

import sys, logging, threading, time

sys.path.insert(0, '../..')
from django.core.management import setup_environ
from ngt import settings
setup_environ(settings)
from models import Reaper
from ngt.jobs.models import Job, JobSet
from django.db.models import Q

logger = logging.getLogger('feeder')
logger.setLevel(logging.INFO)
logging.getLogger('job_models').setLevel(logging.DEBUG)

JOB_MIN = 10
JOB_MAX = 60
METER_POLL_INTERVAL = 1 # seconds
greenlight = threading.Event()
greenlight.set()
dblock = threading.Lock()

def enqueue_jobs():
    ''' Loop and enqueue jobs if the maximum queue size hasn't been exceeded '''
    logger.debug("Job dispatch is launching.")
    Job.objects.filter(status='queued').update(status='requeue')
    while True:
        dblock.acquire()
        active_jobsets = JobSet.objects.filter(active=True)
        jobset_count = active_jobsets.count()
        dblock.release()
        for jobset in active_jobsets:
            try:
                greenlight.wait()
                dblock.acquire()
                job = jobset.jobs.filter(Q(status='new') | Q(status='requeue') )[0]
                job.enqueue()
                dblock.release()
                time.sleep(0.01)
            except IndexError:
                jobset_count -= 1
                if jobset_count == 0:
                    logger.info("Ran out of jobs to enqueue. Dispatch thread will exit.")
                    return True
                    
                    
class TrafficLight(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.daemon = True
        self.name = "Traffic Light"
        
    def run(self):
        logger.info("%s starting" % self.name)
        while True:
            time.sleep(METER_POLL_INTERVAL)
            dblock.acquire()
            running_jobs = Job.objects.filter(status="queued").count()
            dblock.release()
            print "%d JOBS QUEUED." % running_jobs
            if running_jobs >= JOB_MAX:
                greenlight.clear()
                logger.info("Light is red.")
            else:
                if not greenlight.is_set() and running_jobs <= JOB_MIN:
                    greenlight.set()
                    logger.info("Light is green.")
            
                    
if __name__ == '__main__':
    print "OK go."
    trafficlight = TrafficLight()
    trafficlight.start()
    enqueue_jobs()
