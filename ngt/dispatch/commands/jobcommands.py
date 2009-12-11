class JobCommand(object):
    
    name = None
    number_of_args = 0
    
    @classmethod
    def check_readiness(klass, job):
        ''' Return True if the system is ready to process this job, False otherwise. '''
        return True
    
    @classmethod    
    def preprocess_job(klass, job):
        return job
    
    @classmethod
    def postprocess_job(klass, job, state):
        return job
        
import re
from amqplib.client_0_8 import Message
from ngt.messaging.messagebus import MessageBus
from ngt import protocols
from ngt.protocols import protobuf, dotdict

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
