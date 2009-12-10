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
from amqplib.client_0_8 imort Message
from messaging.messagebus import MessageBus
import protocols
import protocols.rpc_services
from protocols import protobuf, dotdict

class MosaicJobCommand(JobCommand): 
    name = 'mosaic'
    number_of_args = 2
    current_footprints = {}
    
    messagebus = MessageBus()
    messagebus.exchange_declare('ngt.platefile.index','direct')

    @classmethod
    def check_readiness(klass, job):
        footprint = job.assets[0].footprint.prepared
        for other_footprint in klass.current_footprints:
            if other_footprint.touches(footprint):
                return False
        else:
            return True

    @classmethod
    def preprocess_job(klass, job):
        klass.current_footprints[job.uuid] = job.assets[0].footprint.prepared
        return job

    @classmethod
    def get_plate_info(output):
        m = re.search('Transaction ID: (\d+)', output)
        assert m
        transaction_id = int(m.groups()[0])
        m = re.search('Platefile ID: (\d+)', output)
        assert m
        platefile_id = int(m.groups()[0])
        
        return transaction_id, platefile_id

    @classmethod
    def postprocess_job(klass, job, state):
        del klass.current_footprints[job.uuid]
        if state == 'failed':
            transaction_id, platefile_id = klass.get_plate_info(job.output)
            idx_transaction_failed = {
                'platefile_id': platefile_id,
                'transaction_id': transaction_id
            }
            request = {
                'requestor': '',
                'method': 'IndexTransactionFailed',
                'payload': protocols.pack(protobuf.IndexTransactionFailed, idx_transaction_failed),
            }
            msg = Message(protocols.pack(protobuf.RpcRequestWrapper, request)
            klass.messagebus.basic_publish(msg, exchange='ngt.platefile.index', routing_key='index')
        
        return job