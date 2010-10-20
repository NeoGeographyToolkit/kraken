import re
import shlex
from amqplib.client_0_8 import Message
from ngt.messaging.messagebus import MessageBus
from ngt import protocols
from ngt.protocols import protobuf, dotdict
from ngt.jobs.models import Job, JobSet
import logging
logger = logging.getLogger('dispatch')

from django.db import transaction

def minmax(iter):
    '''Find the minimum and maximum values in one pass'''
    min = max = iter.next()
    for i in iter:
        if i < min:
            min = i
        if i > max:
            max = i
    return (min, max)

class JobCommand(object):
    """ 
        Base Class and prototype 
        JobCommand subclasses wrap around a Job model instance and provide job-type specific functionality.
        JobCommand instances delegate attribute access to their bound Job instance.
    """
    
    commandname = None
    number_of_args = 0
    
    def __init__(self, job):
        self.job = job
        
    def __getattr__(self, name): # Deletgate any method / attributes not found to the Job model instance
        return object.__getattribute__(self.job, name)

    def __setattr__(self, name, value): # Delegate attribute setting to the Job model instance if that attribute is defined there.
        if 'job' in self.__dict__ and hasattr(self.job, name):
            setattr(self.job, name, value)
        else:
            object.__setattr__(self, name, value)
    
    def check_readiness(self):
        ''' Return True if the system is ready to process this job, False otherwise. '''
        logger.debug("%s:%s is ready to run." % (self.commandname, self.job.uuid[:8]))
        return True
    
    def preprocess(self):
        return self.job
    
    def postprocess(self):
        return self.job
        
    def build_arguments(self, **kwargs):
        if self.job.assets.all():
            return [self.job.assets.all()[0].file_path]
        else:
            return []
            
class RetryingJobCommand(JobCommand):
    commandname='_retry_abc'
    failures = {}
    max_failures = 3
    
    @transaction.commit_on_success
    def postprocess(self):
        if self.job.status == 'failed':
            if self.job.id in self.__class__.failures:
                self.__class__.failures[self.job.id] += 1
            else:
                self.__class__.failures[self.job.id] = 1
            if self.__class__.failures[self.job.id] <= self.__class__.max_failures:
                logger.debug("Job %d failed.  Requeueing." % self.job.id)
                self.job.status = 'requeue'
            else:
                logger.debug("Job %d failed.  Max number of requeues exceeded." % self.job.id)
        return self.job
            
        
class MipMapCommand(RetryingJobCommand):
    commandname = 'mipmap'

    def build_arguments(self, **kwargs):
        args = "-t %s %s -o %s" % (self.job.transaction_id, kwargs['file_path'], kwargs['platefile'])
        return args.split(' ')

    def postprocess(self):
        ''' All MipMap failures will be made nonblocking. '''
        self.job = super(MipMapCommand, self).postprocess(self.job)
        if self.job.status == 'failed':
            self.job.status = 'failed_nonblocking'

class moc2plateCommand(RetryingJobCommand):
    commandname = 'moc2plate'

    def build_arguments(self, **kwargs):
        args = "%s %s -t %d" % (kwargs['file_path'], kwargs['platefile'], self.job.transaction_id)
        return args.split(' ')
        
class hirise2plateCommand(RetryingJobCommand):
    commandname = 'hirise2plate'

    def build_arguments(self, **kwargs):
        args = "%s %s -t %d" % (kwargs['file_path'], kwargs['platefile'], self.job.transaction_id)
        return args.split(' ')

class ctx2plate(JobCommand):
    commandname = 'ctx2plate'
        
class Snapshot(RetryingJobCommand):
    commandname = 'snapshot'
    
    def build_arguments(self, **kwargs):
        ### kwargs expected:
        # region (4-tuple)
        # level (integer)
        # transaction_range (2-tuple)
        # platefile (string)
        ###
    
        regionstr = '%d,%d:%d,%d' % kwargs['region'] # expects a 4-tuple
        return [
            '-t', str(self.job.transaction_id),
            '--region', '%s@%d' % (regionstr, kwargs['level']),
            '--transaction-range', '%d:%d' % kwargs['transaction_range'],
            kwargs['platefile']
        ]

class StartSnapshot(JobCommand):
    commandname = 'start_snapshot'
    
    def build_arguments(self, **kwargs):
        ### kwargs expected:
        # transaction_range (2-tuple)
        # platefile (string)
        ###
        
        t_range = kwargs['transaction_range'] # expect a 2-tuple
        description = "Snapshot of transactions %d --> %d" % t_range
        args = ['--start', '"%s"' % description, '-t', str(self.job.transaction_id), kwargs['platefile']]
        return args

    def _generate_partitions(self, level):
        '''
        Give all the  partitions of a 2**level tilespace.
        Divide the space into 32x32 regions
        '''
        print '\n\n****** Generating Partitions!!!!! ******\n\n'
        sqrt_regions = 32 # number of regions to divide each side by (i.e. there will be sqrt_regions**2 regions)
        if 2**level <= sqrt_regions:
            yield(0, 0, 2**level, 2**level)
        else:
            tiles_per_side = 2**level / sqrt_regions
            for i in range(sqrt_regions):
                for j in range(sqrt_regions):
                    yield (i*tiles_per_side, j*tiles_per_side, (i+1)*tiles_per_side, (j+1)*tiles_per_side)
                    
    def _get_maxlevel(self, output):
        pat = re.compile('Plate has (\d+) levels')
        match = pat.search(output)
        assert match
        return int(match.groups()[0])

            
    
    @transaction.commit_on_success
    def postprocess(self):
        # get platefile from job arguments
        pfpattern = re.compile('pf:')
        args = shlex.split(str(self.job.command_string))
        platefile = filter(pfpattern.match, args)[0]

        # parse pyramid depth (max level) from job output
        maxlevel = self._get_maxlevel(self.job.output)
        # get corresponding end_snapshot job
        endjob = self.job.jobset.jobs.get(command='end_snapshot', transaction_id=self.job.transaction_id)
        #snapjobset = JobSet.objects.filter('snapshots').latest('pk')
        snapjobset = self.job.jobset
        # spawn regular snapshot jobs.  add as dependencies to end_snapshot job
        #transids = [d.transaction_id for d in job.dependencies.all()]
        #job_transaction_range = (min(transids), max(transids))
        logger.info("start_snapshot executed.  Generating snapshot jobs")
        job_transaction_range = minmax(d.transaction_id for d in self.job.dependencies.all())
        jcount = 0
        for level in range(maxlevel):
            for region in self._generate_partitions(level):
                logger.debug("Generating snapshot job for region %s" % str(region))
                #print "Generating snapshot job for region " + str(region) + " at " + str(level)
                snapjob = Job(
                    command = 'snapshot',
                    transaction_id = self.job.transaction_id,
                    jobset = snapjobset,
                )
                snapjob.arguments = snapjob.wrapped().build_arguments(
                    region = region,
                    level = level,
                    transaction_range = job_transaction_range,
                    platefile = platefile,
                )
                snapjob.save()
                endjob.dependencies.add(snapjob)
                
        return self.job
        
    
        
class EndSnapshot(JobCommand):
    commandname = 'end_snapshot'
    
    def build_arguments(self, **kwargs):
        args = ['--finish', '-t', str(self.job.transaction_id), kwargs['platefile']]
        return args
        
    

class MosaicJobCommand(JobCommand): 
    commandname = 'mosaic'
    number_of_args = 2
    current_footprints = {}
    
    messagebus = MessageBus()
    messagebus.exchange_declare('ngt.platefile.index','direct')

    def check_readiness(self):
        if self.job.assets.all()[0].footprint:
            footprint = self.job.assets.all()[0].footprint.prepared
            for other_footprint in self.current_footprints.values():
                if other_footprint.touches(footprint):
                    return False
            else:
                return True
        else:
            return True

    def preprocess(self):
        if self.job.assets.all()[0].footprint:
            self.current_footprints[self.job.uuid] = self.job.assets.all()[0].footprint.prepared
        return self.job

    def _get_plate_info(self, output):
        m = re.search('Transaction ID: (\d+)', output)
        if m:
            transaction_id = int(m.groups()[0])
        else:
            transaction_id = None
        m = re.search('Platefile ID: (\d+)', output)
        if m:
            platefile_id = int(m.groups()[0])
        else:
            platefile_id = None
        return transaction_id, platefile_id

    def postprocess(self):
        if self.job.uuid in self.current_footprints:
            del self.current_footprints[self.job.uuid]
        if self.job.status == 'failed':
            transaction_id, platefile_id = self._get_plate_info(self.job.output)
            if transaction_id and platefile_id:
                idx_transaction_failed = {
                    'platefile_id': platefile_id,
                    'transaction_id': transaction_id
                }
                request = {
                    'sequence_number': 0,
                    'requestor': '',
                    'method': 'TransactionFailed',
                    'payload': protocols.pack(protobuf.IndexTransactionFailed, idx_transaction_failed),
                }
                msg = Message(protocols.pack(protobuf.BroxtonRequestWrapper, request))
                self.messagebus.basic_publish(msg, exchange='ngt.platefile.index_0', routing_key='index')
        
        return self.job
        
        
####
# For testing dependencies...
####

class Test(RetryingJobCommand):
    commandname = 'test'

class Horse(JobCommand):
    ''' Horse jobs are only ready 90% of the time and take forever to get going. '''
    commandname = 'test_horse'
    
    def postprocess(self):
        logger.info( "Horse %d ran." % self.id )
        return self.job
        
    def preprocess(self):
        import time
        time.sleep(1)
        logger.info( "About to run Horse %d." % self.id )
        return self.job
        
    def check_readiness(self):
        ''' Be ready 10% of the time. '''
        import random
        if random.randint(0,9) == 0:
            logger.debug("%s:%s is ready to run." % (self.commandname, self.job.uuid[:8]))
            return True
        else:
            logger.debug("%s:%s is not ready to run yet." % (self.commandname, self.job.uuid[:8]))
            return False
        
        
class Cart(JobCommand):
    '''Cart jobs depend on Horse jobs.'''
    commandname = 'test_cart'
    
    def postprocess(self):
        print "Cart ran."
        return self.job
