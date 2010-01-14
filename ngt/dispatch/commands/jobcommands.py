import json
import re
from amqplib.client_0_8 import Message
from ngt.messaging.messagebus import MessageBus
from ngt import protocols
from ngt.protocols import protobuf, dotdict
from ngt.jobs.models import Job, JobSet
import logging
logger = logging.getLogger('dispatch')

from django.db import transaction

PLATEFILE = 'pf://wwt10one/index/moc_v1.plate'

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
    """ Base Class and prototype """
    
    name = None
    number_of_args = 0
    
    @classmethod
    def check_readiness(klass, job):
        ''' Return True if the system is ready to process this job, False otherwise. '''
        logger.debug("%s:%s is ready to run." % (klass.name, job.uuid[:8]))
        return True
    
    @classmethod
    def preprocess_job(klass, job):
        return job
    
    @classmethod
    def postprocess_job(klass, job, state):
        return job
        
    @classmethod
    def build_arguments(klass, job, **kwargs):
        if job.assets.all():
            return [job.assets.all()[0].file_path]
        else:
            return []
            
        
class MipMapCommand(JobCommand):
    name = 'mipmap'

    @classmethod
    def build_arguments(klass, job, **kwargs):
        args = "-t %s %s -o %s" % (job.transaction_id, kwargs['file_path'], kwargs['platefile'])
        return args.split(' ')
        
class Snapshot(JobCommand):
    name = 'snapshot'
    
    @classmethod
    def build_arguments(klass, job, **kwargs):
        ### kwargs expected:
        # region (4-tuple)
        # level (integer)
        # transaction_range (2-tuple)
        # platefile (string)
        ###
    
        regionstr = '%d,%d:%d,%d' % kwargs['region'] # expects a 4-tuple
        return [
            '-t', str(job.transaction_id),
            '--region', '%s@%d' % (regionstr, kwargs['level']),
            '--transaction-range', '%d:%d' % kwargs['transaction_range'],
            kwargs['platefile']
        ]

class StartSnapshot(JobCommand):
    name = 'start_snapshot'
    
    @classmethod
    def build_arguments(klass, job, **kwargs):
        ### kwargs expected:
        # transaction_range (2-tuple)
        # platefile (string)
        ###
        
        t_range = kwargs['transaction_range'] # expect a 2-tuple
        description = "Snapshot of transactions %d --> %d" % t_range
        args = ['--start', '"%s"' % description, '-t', str(job.transaction_id), kwargs['platefile']]
        return args
    """    
    @classmethod
    def _generate_partitions(klass, level):
        '''
        Give all the 1024x1024 partitions of a 2**level tilespace.
        Yields 4-tuples
        '''
        width = 2**level
        if width < 1024:
            yield (0,width, 0,width)
        else:
            for i in range(width / 1024):
                for j in range(width / 1024):
                    yield (i*1024, (i+1)*1024-1, j*1024, (j+1)*1024-1)
    """
                    
    @classmethod
    def _generate_partitions(klass, level):
        '''
        Give all the  partitions of a 2**level tilespace.
        Divide the space into 16x16 regions
        '''
        tiles = 16 # actually there are tiles**2 tiles
        if 2**level <= tiles:
            yield(0, 2**level, 0, 2**level)
        else:
            side = 2**level / tiles
            for i in range(tiles):
                for j in range(tiles):
                    yield (i*side, (i+1)*side-1, j*side, (j+1)*side-1)
                    
    @classmethod
    def _get_maxlevel(klass, output):
        pat = re.compile('Plate has (\d+) levels')
        match = pat.search(output)
        assert match
        return int(match.groups()[0])

            
    
    @classmethod
    @transaction.commit_on_success
    def postprocess_job(klass, job, state):
        # parse pyramid depth (max level) from job output
        maxlevel = klass._get_maxlevel(job.output)
        # get corresponding end_snapshot job
        endjob = job.jobset.jobs.get(command='end_snapshot', transaction_id=job.transaction_id)
        #snapjobset = JobSet.objects.filter('snapshots').latest('pk')
        snapjobset = job.jobset
        # spawn regular snapshot jobs.  add as dependencies to end_snapshot job
        #transids = [d.transaction_id for d in job.dependencies.all()]
        #job_transaction_range = (min(transids), max(transids))
        logger.info("start_snapshot executed.  Generating snapshot jobs")
        job_transaction_range = minmax(d.transaction_id for d in job.dependencies.all())
        jcount = 0
        for level in range(maxlevel + 1):
            for region in klass._generate_partitions(level):
                logger.debug("Generating snapshot job for region %s" % str(region))
                snapjob = Job(
                    command = 'snapshot',
                    transaction_id = job.transaction_id,
                    jobset = snapjobset,
                )
                snapjob.arguments = json.dumps(Snapshot.build_arguments(
                    job,
                    region = region,
                    level = level,
                    transaction_range = job_transaction_range,
                    platefile = PLATEFILE,
                ))
                snapjob.save()
                endjob.dependencies.add(snapjob)
                
        return job
        
    
        
class EndSnapshot(JobCommand):
    name = 'end_snapshot'
    
    @classmethod
    def build_arguments(klass, job, **kwargs):
        args = ['--finish', '-t', str(job.transaction_id)]
        return args
        
    

class MosaicJobCommand(JobCommand): 
    name = 'mosaic'
    number_of_args = 2
    current_footprints = {}
    
    messagebus = MessageBus()
    messagebus.exchange_declare('ngt.platefile.index','direct')

    @classmethod
    def check_readiness(klass, job):
        if job.assets.all()[0].footprint:
            import pdb; pdb.set_trace()
            footprint = job.assets.all()[0].footprint.prepared
            for other_footprint in klass.current_footprints.values():
                if other_footprint.touches(footprint):
                    return False
            else:
                return True
        else:
            return True

    @classmethod
    def preprocess_job(klass, job):
        if job.assets.all()[0].footprint:
            klass.current_footprints[job.uuid] = job.assets.all()[0].footprint.prepared
        return job

    @classmethod
    def get_plate_info(klass, output):
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

    @classmethod
    def postprocess_job(klass, job, state):
        if job.uuid in klass.current_footprints:
            del klass.current_footprints[job.uuid]
        if state == 'failed':
            transaction_id, platefile_id = klass.get_plate_info(job.output)
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
                klass.messagebus.basic_publish(msg, exchange='ngt.platefile.index_0', routing_key='index')
        
        return job
        
        
####
# For testing dependencies...
####
class Fjord(JobCommand):
    ''' Fjord jobs are only ready 90% of the time and take forever to get going. '''
    name = 'test_fjord'
    
    @classmethod
    def postprocess_job(klass, job, state):
        print "Fjord ran."
        return job
        
    @classmethod
    def preprocess_job(klass, job):
        import time
        time.sleep(1)
        return job
        
    @classmethod
    def check_readiness(klass, job):
        ''' Be ready 10% of the time. '''
        import random
        if random.randint(0,9) == 0:
            logger.debug("%s:%s is ready to run." % (klass.name, job.uuid[:8]))
            return True
        else:
            logger.debug("%s:%s is not ready to run yet." % (klass.name, job.uuid[:8]))
            return False
        
        
class Bjorn(JobCommand):
    '''Bjorn jobs depend on Fjord jobs.'''
    name = 'test_bjorn'
    
    @classmethod
    def postprocess_job(klass, job, state):
        print "Bjorn ran."
        return job
