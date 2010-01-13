from ngt import settings
if not settings.DISABLE_GEO:
    from django.contrib.gis.db import models
else:
    from django.db import models
import os, time, hashlib, datetime
import uuid
from ngt.messaging.messagebus import MessageBus
import json
from ngt import protocols
from ngt.protocols import protobuf
#from ngt.assets.models import Asset, DATA_ROOT

import logging
logger = logging.getLogger('job_models')

messagebus = MessageBus()

messagebus.channel.exchange_declare(exchange="Job_Exchange", type="direct", durable=True, auto_delete=False,)
messagebus.channel.queue_declare(queue='reaper.generic', auto_delete=False)
messagebus.channel.queue_bind(queue='reaper.generic', exchange='Job_Exchange', routing_key='reaper.generic')
"""
# RPC Service to dispatch
REPLY_QUEUE_NAME = 'jobmodels'
JOB_EXCHANGE_NAME = 'Job_Exchange'
chan.queue_declare(queue=self.REPLY_QUEUE_NAME, durable=False, auto_delete=True)
chan.queue_bind(self.REPLY_QUEUE_NAME, self.JOB_EXCHANGE_NAME, routing_key=self.REPLY_QUEUE_NAME)
dispatch_rpc_channel = protocols.rpc_services.RpcChannel(self.JOB_EXCHANGE_NAME, self.REPLY_QUEUE_NAME, 'dispatch')
dispatch = protobuf.DispatchCommandService_Stub(dispatch_rpc_channel)
amqp_rpc_controller = protocols.rpc_services.AmqpRpcController()
"""

class Job(models.Model):
    uuid = models.CharField(max_length=32, null=True)
    jobset = models.ForeignKey('JobSet', related_name="jobs")
    transaction_id = models.IntegerField(null=True)
    command = models.CharField(max_length=64)
    arguments = models.TextField(default='') # an array seriaized as json
    status = models.CharField(max_length=32, default='new')
    processor = models.CharField(max_length=32, null=True, default=None)
    assets = models.ManyToManyField('assets.Asset', related_name='jobs')
    output = models.TextField(null=True)

    time_started = models.DateTimeField(null=True, default=None)
    time_ended = models.DateTimeField(null=True, default=None)
    pid = models.IntegerField(null=True)
    
    dependencies = models.ManyToManyField('Job', symmetrical=False)
    
    creates_new_asset = models.BooleanField(default=False) # if this is set, the dispatcher will create a new asset when the job is completed
    outfile_argument_index = models.SmallIntegerField(default=1) # index of the output filename in the argument list.  Used to generate output asset records.
    
    if not settings.DISABLE_GEO:
            footprint = models.PolygonField(null=True, srid=949900)
    
    def _generate_uuid(self):
        '''Returns a unique job ID that is the MD5 hash of the local
        hostname, the local time of day, and the command & arguments for this job.'''
        return uuid.uuid1().hex
    
    def __unicode__(self):
        return self.uuid

    @property
    def command_string(self):
        return self.command + ' ' + ' '.join(json.loads(self.arguments))
        
    def ended(self):
        ''' Return True if the job has run and met and end condition, False otherwise. '''
        end_statuses = ('complete', 'failed', 'ended')
        if self.status in end_statuses:
            return True
        else:
            return False
            
    def dependencies_met(self):
        ''' 
            Return True if all dependencies are met, False otherwise.
            A dependency is met if the depending job has ended.
        '''
        return all([ dep.ended() for dep in self.dependencies.all() ])
    
    def enqueue(self):
        cmd = {
            'uuid': self.uuid,
            'command': self.command,
            'args': json.loads(self.arguments)
        }
        message_body = protocols.pack(protobuf.Command, cmd)
        self.status = 'queued'
        logger.debug("job.enqueue: about to save record")
        self.save()
        logger.debug("job.enqueue: record saved. Publishing to wire.")   
        messagebus.publish(message_body, exchange='Job_Exchange', routing_key='reaper.generic') #routing key is the name of the intended reaper type
        print "Enqueued %s" % self.uuid
        
    def spawn_output_asset(self):
        """ Creates a new asset record for the job's output file. 
            Assumes that the output filename will be the second parameter in the output list
        """
        assert self.assets.count() == 1
        asset_o = self.assets.all()[0]
        asset_n = Asset()
        asset_n.__dict = asset_o.__dict__
        asset_n.id = None
        asset_n.is_original = False
        asset_n.creator_job = self
        args = json.loads(self.arguments)
        asset_n.relative_file_path = args[self.outfile_argument_index].replace(DATA_ROOT,'')
        assert os.path.exists(asset_n.file_path)
        asset_n.class_label = self.jobset.output_asset_label or self.jobset.name
        asset_n.save()
        asset_n.parents.add(asset_o)
        
    
    
def set_uuid(instance, **kwargs):
    if not instance.uuid:
        instance.uuid = instance._generate_uuid()
models.signals.pre_save.connect(set_uuid, sender=Job)

class JobSet(models.Model):
    name = models.CharField(max_length=256)
    assets = models.ManyToManyField('assets.Asset') # this collection of assets can be used to populate jobs
    #jobs = models.ManyToManyField(Job, editable=False) # now a foreign key in the Job model
    status = models.CharField(max_length=32, default='new')
    command = models.CharField(max_length=64)
    active = models.BooleanField(default=False)
    priority = models.IntegerField(default=0)
    
    output_asset_label = models.CharField(max_length=256, null=True, default=None) # this is the label that will be applied to assets generated by jobs in this set
    
    def __unicode__(self):
        return "<%d: %s>" % (self.id, self.name)
        
    def simple_populate(self, creates_new_asset=True):
        """ Create one-parameter jobs for each of this batch's assets
            Only really useful for testing.
        """
        print "Creating jobs for %s" % str(self.assets.all())
        for asset in self.assets.all():
            print "About to create a job for %s" % str(asset)
            self.jobs.create(
                command=self.command, 
                arguments='["%s"]' % asset.file_path, #json-decodable lists of one
                creates_new_asset = creates_new_asset,
            )
    
    def execute(self):
        #self.simple_populate()
        self.status = "dispatched"
        for job in self.jobs.filter(status='new'):
            job.enqueue()
    def reset(self):
        self.jobs.update(status='new')
            
from ngt.assets.models import Asset, DATA_ROOT # putting this here helps avoid circular imports

"""
I'd like jobs to be populated from the JobSet's properties by a post-save signal...
But this won't work because the related objects in jobbatch.assests don't get created until after the post_save signal has fired.

def populate_jobs(instance, created, **kwargs):
    print "populate_jobs fired: %s" % str(created)
    if created:
        instance.simple_populate() #just one asset per job, for now.
models.signals.post_save.connect(populate_jobs, sender=JobSet)
"""
