from django.db import models
from django.db import transaction
from ngt.jobs.models import Job, JobSet

# Let's see if we can do this without modelling Nodes...
#class Node(models.Model):
#    """ A registered node from which we can launch reapers. """
#    ip = models.IPAddressField()
#    start_time = models.DateTimeField(auto_now_add=True)
#    
#    def are_you_alive(self):
#        """ Send the node controller a message or signal to see if it is still alive."""
#        pass


class ReaperManager(models.Manager):
    ''' Manage soft deletion and expiration of reapers '''
    #def get_query_set(self):
    #    return super(ReaperManager, self).get_query_set().filter(deleted=False, expired=False)
    def deleted(self):
        return super(ReaperManager, self).get_query_set().filter(deleted=True)
    def expired(self):
        return super(ReaperManager, self).get_query_set().filter(expired=True)
    def any(self):
        return super(ReaperManager, self).all()
        
class Reaper(models.Model):
    any_objects = models.Manager()
    objects = ReaperManager()
    class Meta:
        app_label = 'dispatch'
        
    uuid = models.CharField(max_length=32, unique=True)
    type = models.CharField(max_length=128, default='generic')
    creation_time = models.DateTimeField(auto_now_add=True)
    modification_time = models.DateTimeField(auto_now=True)
    last_job_finished = models.DateTimeField(null=True, default=None)
    current_job = models.ForeignKey(Job, null=True)
    status = models.CharField(max_length=128, default='up')
    #ip = models.IPAddressField(null=True)
    hostname = models.CharField(max_length=64, null=True)
    jobcount = models.IntegerField(default=0)
    
    # Soft Deletion and expiration
    deleted = models.BooleanField(default=False)
    expired = models.BooleanField(default=False)
    
    def soft_delete(self):
        self.deleted = True
        self.save()

####
# Utilities for manual maintenence
###
@transaction.commit_on_success
def requeue_host_jobs(hostname):
    '''
    For use in the event of a processing node failure.
    find all the jobs that were being processed by a given hosthame and requeue them.
    '''
    reaperids = [r.uuid for r in Reaper.objects.filter(deleted=False, expired=False, hostname=hostname)]
    print "%d reapers are registered on %s" % (len(reaperids), hostname)
    active_jobsets = JobSet.objects.filter(active=True).values_list('id', flat=True)
    processing_jobs = Job.objects.filter(jobset__id__in=active_jobsets, status_enum=Job.StatusEnum.PROCESSING)
    hostjobs = [j for j in processing_jobs if j.processor in reaperids]
    print "%d jobs were processing on host %s." % (len(hostjobs), hostname) 
    print "Last Job started at %s" % str(max(j.time_started for j in hostjobs))
    print "Type 'okeydoke' to requeue."
    input = raw_input()
    if input == 'okeydoke':
        for job in hostjobs:
            job.status = 'requeue'
            job.save()
        print "Done."
    else:
        print "ABORT!"
