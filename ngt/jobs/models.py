from ngt import settings
if not settings.DISABLE_GEO:
    from django.contrib.gis.db import models
else:
    from django.db import models
from django import db
from django.db import transaction
import os, time, hashlib, datetime
import uuid
#import json
from ngt.django_extras.fields import JSONField
#from ngt.assets.models import Asset, DATA_ROOT

import logging
logger = logging.getLogger('job_models')

class Job(models.Model):

    ###
    # Status Enumeration
    class StatusEnum(object):
        NEW = 0
        REQUEUE = 1
        ENQUEUED = 2
        PROCESSING = 3
        COMPLETE = 4
        FAILED = 5
        FAILED_NONBLOCKING = 6
        end_states = (COMPLETE, FAILED_NONBLOCKING)
        #end_states = (COMPLETE, )
        
        @classmethod
        def reverse(klass, value):
            for k,v in klass.__dict__.items():
                if v == value: return k.lower()
            else:
                raise ValueError("Value %d not found in %s." % (value, klass.__name__))
        
    class EnumDescriptor(object):
        '''
        A sneaky way to use an enum values without 
        hunting down and changing all the existing code that gets and sets string values.
        '''
        def __init__(self, enum_field_name, enum_class):
            self.enum_field_name = enum_field_name
            self.enum_class = enum_class
        def __get__(self, instance, owner):
            value = getattr(instance, self.enum_field_name)
            return self.enum_class.reverse(value)
        
        def __set__(self, instance, value):
            setattr(instance, self.enum_field_name, getattr(self.enum_class, value.upper()))
    #
    ###

    class ArgumentDescriptor(object):
        '''
        This descriptor emulates the old arguments property.
        Arguments were once stored as a JSON list of strings, rather than a property on a JSON serialized dict.
        It accepts and return a list of strings, but uses the context dict for storage.
        TODO: At some point this needs to be refactored so that individual Job subtypes can carry their own argument templates and use arbitrary context members.
        '''
        def __init__(self, jsonfield_name):
            self.fieldname = jsonfield_name

        def __get__(self, instance, owner):
            json = getattr(instance, self.fieldname)
            if type(json) == list: # This is for backwards compatibility with legacy jobs.
                arguments = json
            elif type(json) == dict:
                arguments = json['arguments']
            else:
                raise ValueError("ListDescriptor encountered a value that deserializes to an unsupported type.")
            assert type(arguments) == list
            return arguments

        def __set__(self, instance, value):
            if type(value) != list:
                raise ValueError("ListDescriptor requires a list value.")

            # Default to a dict-style representation if the storage field is unset
            # Will overwrite an existing list representation if present
            if type(getattr(instance, self.fieldname)) in (type(None), list):
                setattr(instance, self.fieldname, dict()) 
            getattr(instance, self.fieldname)['arguments'] = value

    
    uuid = models.CharField(max_length=32, null=True)
    jobset = models.ForeignKey('JobSet', related_name="jobs")
    transaction_id = models.IntegerField(null=True)
    command = models.CharField(max_length=64)
    #arguments = models.TextField(default='') # an array seriaized as json
    context = JSONField(default={})
    arguments = ArgumentDescriptor('context')
    #status = models.CharField(max_length=32, default='new')
    status_enum = models.IntegerField(db_column='status_int', default=0)
    status = EnumDescriptor('status_enum', StatusEnum)
    processor = models.CharField(max_length=32, null=True, default=None)
    assets = models.ManyToManyField('assets.Asset', related_name='jobs')
    output = models.TextField(null=True)

    time_started = models.DateTimeField(null=True, default=None)
    time_ended = models.DateTimeField(null=True, default=None)
    pid = models.IntegerField(null=True)
    #ended = models.BooleanField(default=False)
    
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
        if self.command and self.uuid:
            return self.command + ' ' + self.uuid
            return "<Job: %s %s>" % (self.command, self.uuid)
        else:
            return "<Job: new>"

    @property
    def command_string(self):
        if type(self.arguments) == list:
            return self.command + ' ' + ' '.join(self.arguments)
        else:
            raise NotImplementedError

    @property
    def runtime(self):
        return self.time_ended - self.time_started

    @property
    def reaper(self):
        from ngt.dispatch.models import Reaper
        return Reaper.objects.get(uuid=self.processor)       
            
    def dependencies_met(self):
        ''' 
            Return True if all dependencies are met, False otherwise.
            A dependency is met if the depending job has ended.
        '''
        for dep in self.dependencies.all():
            if not dep.ended:
                return False
        else:
            return True
            
    @property
    def ended(self):
        return self.status_enum in Job.StatusEnum.end_states
        
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
#def set_ended(instance, **kwargs):
#    if instance.status in ('complete','failed','ended'):
#        instance.ended = True
#    else:
#        instance.ended = False
models.signals.pre_save.connect(set_uuid, sender=Job)
#models.signals.pre_save.connect(set_ended, sender=Job)

class JobSet(models.Model):
    name = models.CharField(max_length=256)
    assets = models.ManyToManyField('assets.Asset') # this collection of assets can be used to populate jobs
    #jobs = models.ManyToManyField(Job, editable=False) # now a foreign key in the Job model
    command = models.CharField(max_length=64)
    active = models.BooleanField(default=False)
    priority = models.IntegerField(default=0)
    
    output_asset_label = models.CharField(max_length=256, null=True, default=None) # this is the label that will be applied to assets generated by jobs in this set
    
    def __unicode__(self):
        if self.id and self.name:
            return "<JS %d: %s>" % (self.id, self.name)
        else:
            return "<Jobset: No Name>" 
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


    ####
    # Convenience Methods for manual jobset wrangling.
    # Please do not use these in any critical scripts, as the interfaces are subject to change at any time.
    ####
    def status(self):
        qry = "SELECT status_int, count(*) FROM jobs_job WHERE jobset_id = %d GROUP BY status_int" % self.id
        cursor = db.connection.cursor()
        cursor.execute(qry)
        counts = {}
        for row in cursor.fetchall():
            counts[Job.StatusEnum.reverse(row[0])] = row[1]
        return counts

        
    @transaction.commit_on_success
    def reset(self):
        self.jobs.update(status_enum=Job.StatusEnum.NEW)
        if 'snapshot' in self.name.lower():
            self.jobs.filter(command='snapshot').delete()

    def requeue(self):
        return self.jobs.filter(status_enum=Job.StatusEnum.PROCESSING).update(status_enum=Job.StatusEnum.REQUEUE)

    @classmethod
    @transaction.commit_on_success
    def reset_active(klass):
        for js in klass.objects.filter(active = True):
            js.reset()

    @classmethod
    def get(klass, jobset):
        if type(jobset) == klass:
            return jobset
        elif type(jobset) == int:
            return klass.objects.get(pk=jobset)
        else:
            raise ArgumentError("Expected a JobSet or int.")

    @classmethod
    def activate(klass, jobset):
        js = klass.get(jobset)
        js.active = True
        js.save()
        print "%s activated." % str(js)

    @classmethod
    def deactivate(klass, jobset):
        js = klass.get(jobset)
        js.active = False
        js.save()
        print "%s deactivated." % str(js)
            
def active_jobsets():
    jobsets = JobSet.objects.filter(active=True)
    return jobsets
def status():
    jobsets = active_jobsets()
    from pprint import pprint
    pprint( dict( [(js, js.status()) for js in jobsets] ) )

from ngt.assets.models import Asset, DATA_ROOT # putting this here helps avoid circular imports

