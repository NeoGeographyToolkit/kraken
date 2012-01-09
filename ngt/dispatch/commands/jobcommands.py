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
            
        
class Image2PlateCommand(RetryingJobCommand):
    """
    M. Broxton originally referred to image2plate as mipmap (although the command no longer does any mipmapping).
    This command was originally called mipmap as a result.  Later we added a new command called mipmap, that actualy mipmaps.
    To avoid future problems, I've renamed this JobCommand.
    It will break certain old jobsets and jobset creation scripts that rely on the notion that mipmap means image2plate.

    I am so, so sorry.
    """
    commandname = 'image2plate'

    def build_arguments(self, **kwargs):
        args = "-t %s %s -o %s" % (self.job.transaction_id, kwargs['file_path'], kwargs['platefile'])
        return args.split(' ')

    def postprocess(self):
        ''' All MipMap failures will be made nonblocking. '''
        self.job = super(MipMapCommand, self).postprocess(self.job)
        if self.job.status == 'failed':
            self.job.status = 'failed_nonblocking'

class MipMapCommand(JobCommand):
    """
    This calls out the "mipmap" executable and actually mipmaps from one plate level to another.
    If you're running a jobset created by an ancient script, it may not be the command you're looking for.
    See MipMapCommand docstring for how we got into this mess.
    """
    commandname = "mipmap"

    def build_arguments(self, platefile, mm_from_level, transaction_id):
        # mipmap <URL> --level <level to get data from>:<last level to write> -m equi -t <same TID as snapshot>
        return [
            platefile,
            '--level', '%s:0' % mm_from_level,
            '-m', 'equi',
            '-t', str(transaction_id),
        ]


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

class ctx2plateCommand(JobCommand):
    commandname = 'ctx2plate'

    def build_arguments(self, **kwargs):
        for keyword in ('url', 'platefile', 'transaction_id'): # required keywords
            assert keyword in kwargs
        kwargs = dotdict(kwargs)
        arguments = [kwargs.url, kwargs.platefile]
        arguments += ['-t', str(kwargs.transaction_id)]
        if 'downsample' in kwargs and kwargs.downsample:
            arguments.append( "--downsample="+str(kwargs.downsample) )
        if 'normalize' in kwargs and kwargs.normalize:
            arguments.append('--normalize')
        return arguments
        
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
        args =  [
            '-t', str(self.job.transaction_id),
            '--region', '%s@%d' % (regionstr, kwargs['level']),
            '--transaction-range', '%d:%d' % kwargs['transaction_range'],
            kwargs['input_platefile']
        ]
        if 'output_platefile' in kwargs and kwargs['output_platefile']:
            args.append('-o')
            args.append(kwargs['output_platefile'])

        return [ a.strip() for a in args ]

class StartSnapshot(JobCommand):
    commandname = 'start_snapshot'
    
    def build_arguments(self, **kwargs):
        ### kwargs expected:
        # transaction_range (2-tuple)
        # platefile (string)
        ###
        
        t_range = kwargs['transaction_range'] # expect a 2-tuple
        description = "Snapshot of transactions %d --> %d" % t_range
        args = ['--start', '"%s"' % description, '-t', str(self.job.transaction_id), kwargs['input_platefile']]
        if 'output_platefile' in kwargs and kwargs['output_platefile']:
            args.append('-o') 
            args.append(kwargs['output_platefile'])
        return [ a.strip() for a in args ]

    def _generate_partitions(self, level):
        '''
        Give all the  partitions of a 2**level tilespace.
        Divide the space into 32x32 regions
        '''
        print '\n\n****** Generating Partitions!!!!! ******\n\n'
        max_region_size = 128 # root size of a region: we can fit a maximum of 16,384 (128**2) tiles within the address space for a single blob file
        if 2**level <= max_region_size:
            yield(0, 0, 2**level, 2**level)
        else:
            number_of_regions = 2**level / max_region_size
            for i in range(number_of_regions):
                for j in range(number_of_regions):
                    yield (i*max_region_size, j*max_region_size, (i+1)*max_region_size, (j+1)*max_region_size)
                    
    def _get_maxlevel(self, output):
        pat = re.compile('Plate has (\d+) levels')
        match = pat.search(output)
        try:
            assert match
        except AssertionError:
            print "OUTPUT: " + output
            raise
        return int(match.groups()[0])

            
    
    @transaction.commit_on_success
    def postprocess(self):
        # get platefile from job arguments
        pfpattern = re.compile('(pf|amqp|zmq|zmq\+(icp|tcp)):')
        args = shlex.split(str(self.job.command_string))

        input_platefile = None
        output_platefile = None
        for i in range(len(args)):
            if pfpattern.match(args[i]):
                if args[i-1] in ('-o', '--output-plate'):
                    assert not output_platefile
                    output_platefile = args[i]
                else:
                    assert not input_platefile
                    input_platefile = args[i]
            if input_platefile and output_platefile:
                break
        assert input_platefile

        if not output_platefile:
            output_platefile = input_platefile
               
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
        lowest_snapshot_level = self.job.context['lowest_snapshot_level']
        jcount = 0
        for level in range(lowest_snapshot_level, maxlevel):
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
                    input_platefile = input_platefile,
                    output_platefile = output_platefile
                )
                snapjob.save()
                endjob.dependencies.add(snapjob)
        if lowest_snapshot_level > 0:
            mipmapjob = Job(
                command="mipmap",
                transaction_id = self.job.transaction_id,
                jobset = snapjobset,
            )
            mipmapjob.arguments = mipmapjob.wrapped().build_arguments(output_platefile, lowest_snapshot_level, self.job.transaction_id)
            mipmapjob.save()
            mipmapjob.dependencies.add(endjob)
            mipmapjob.save()
                
        return self.job
        
    
        
class EndSnapshot(JobCommand):
    commandname = 'end_snapshot'
    
    def build_arguments(self, **kwargs):
        args = ['--finish', '-t', str(self.job.transaction_id), kwargs['input_platefile']]
        if 'output_platefile' in kwargs and kwargs['output_platefile']:
            args.append('-o')
            args.append(kwargs['output_platefile'])
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
