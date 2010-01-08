import json
import re
from amqplib.client_0_8 import Message
from ngt.messaging.messagebus import MessageBus
from ngt import protocols
from ngt.protocols import protobuf, dotdict
import logging
logger = logging.getLogger('dispatch')

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
            
        
class image2plateCommand(JobCommand):
    name = 'mipmap'

    @classmethod
    def build_arguments(klass, job, **kwargs):
        args = "-t %s %s -o %s" % (job.transaction_id, job.assets.all()[0].file_path, kwargs['platefile'])
        return args.split(' ')
        
class Snapshot(JobCommand):
    name = 'snapshot'
    
    def build_arguments(klass, job, **kwargs):
        ### kwargs expected:
        # region (4-tuple)
        # level (integer)
        # transaction_range (2-tuple)
        # platefile (string)
        ###
    
        regionstr = '%d,%d;%d,%d' % kwargs['region'] # expects a 4-tuple
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
        
    @classmethod
    def postprocess_job(klass, job, state):
        # parse pyramid depth (max level) from job output
        # get corresponding end_snapshot job
        # spawn regular snapshot jobs.  add as depenencies to end_snapshot job
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
