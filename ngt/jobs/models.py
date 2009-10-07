from django.db import models
import os, time, hashlib, datetime
from ngt.messaging.messagebus import MessageBus
from pds.models import Asset
import json
from ngt import protocols

messagebus = MessageBus()

"""
REFACTORED TO just Job...
class RemoteJob(models.Model):
    date_added = models.DateTimeField('date acquired')
    job_id = models.CharField(max_length=64)
    job_string = models.CharField(max_length=4096)

    def generate_unique_id(self, job_str):
        '''Returns a unique job ID that is the MD5 hash of the local
        hostname, the local time of day, and the job string.'''
        hostname = os.uname()[1]
        t = time.clock()
        m = hashlib.md5()
        m.update(str(hostname))
        m.update(str(t))
        m.update(job_str)
        return m.hexdigest()

    def __init__(self, job_string, callback = None):
        self.job_id = self.generate_unique_id(job_string)
        self.date_added = datetime.datetime.now()
        self.job_string = job_string
        print('[' + str(self.date_added) + '] ' + 'Registering NGT job ' + self.job_id +
              ' : ' + self.job_string)
        MessageBus().publish("JOB " + str(self.job_id) +
                                   " EXECUTE " + str(self.job_string))
        
    def __unicode__(self):
        return "NGT job: " + self.job_id + "    Started at " + self.date_added
"""

class Job(models.Model):
    uuid = models.CharField(max_length=64, null=True)
    command = models.CharField(max_length=64)
    arguments = models.TextField(null=True)
    status = models.CharField(max_length=32, default='new')
    
    def _generate_uuid(self):
        '''Returns a unique job ID that is the MD5 hash of the local
        hostname, the local time of day, and the command & arguments for this job.'''
        hostname = os.uname()[1]
        t = time.clock()
        m = hashlib.md5()
        m.update(str(hostname))
        m.update(str(t))
        m.update(self.command)
        m.update(self.arguments)
        return m.hexdigest()
    
    def __unicode__(self):
        return self.uuid
    
    def enqueue(self):
        #message_body = '{"uuid": "%s", "command": "%s", "args": %s}' % (self.uuid, self.command, self.arguments) #JSON object literal
        cmd = protocols.Command()
        cmd.uuid = self.uuid
        cmd.command = self.command
        for a in json.loads(self.arguments):
            cmd.args.append(a)
        message_body = cmd.SerializeToString()
        self.status = 'queued'
        self.save()
        messagebus.publish(message_body, routing_key='command')
        print "Enqueued %s" % self.uuid
        
    
    
def set_uuid(instance, **kwargs):
    if not instance.uuid:
        instance.uuid = instance._generate_uuid()
models.signals.pre_save.connect(set_uuid, sender=Job)

class JobBatch(models.Model):
    name = models.CharField(max_length=256)
    assets = models.ManyToManyField(Asset)
    jobs = models.ManyToManyField(Job, editable=False)
    status = models.CharField(max_length=32, default='new')
    command = models.CharField(max_length=64)
    
    def __unicode__(self):
        return self.name
        
    def simple_populate(self):
        """Create one-parameter jobs for each of this batch's assets"""
        print "Creating jobs for %s" % str(self.assets.all())
        for asset in self.assets.all():
            print "About to create a job for %s" % str(asset)
            self.jobs.create(
                command=self.command, 
                arguments='["%s"]' % asset.image_path, #json-decodable lists of one
            )
    
    def execute(self):
        self.simple_populate()
        self.status = "dispatched"
        for job in self.jobs.filter(status='new'):
            job.enqueue()

"""
I'd like jobs to be populated from the JobBatch's properties by a post-save signal...
But this won't work because the related objects in jobbatch.assests don't get created until after the post_save signal has fired.

def populate_jobs(instance, created, **kwargs):
    print "populate_jobs fired: %s" % str(created)
    if created:
        instance.simple_populate() #just one asset per job, for now.
models.signals.post_save.connect(populate_jobs, sender=JobBatch)
"""