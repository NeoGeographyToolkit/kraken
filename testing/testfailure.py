#!/usr/bin/env python
import re, time, sys
from amqplib.client_0_8 import Message
sys.path.insert(0, '..')
from ngt.messaging.messagebus import MessageBus
from ngt import protocols
from ngt.protocols import protobuf, dotdict
import logging

logging.basicConfig()
logger = logging.getLogger('test')
logger.setLevel(logging.DEBUG)

class FakeJobCommand(object):

    current_footprints = []
    messagebus = MessageBus()
    messagebus.exchange_declare('ngt.platefile.index','direct')

    @classmethod
    def get_plate_info(klass, somevar):
        return (12345,67890)

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
                      'method': 'IndexTransactionFailed',
                      'payload': protocols.pack(protobuf.IndexTransactionFailed, idx_transaction_failed),
                  }
                  msg = Message(protocols.pack(protobuf.BroxtonRequestWrapper, request))
                  klass.messagebus.basic_publish(msg, exchange='ngt.platefile.index', routing_key='index')
                  logger.debug("Message published: " + str(msg))
      
          return job
          
def do_tests():
    job = dotdict()
    job.uuid = 'aeiou12345'
    
    while True:
        logger.info("Simulating postprocess_job")
        FakeJobCommand.postprocess_job(job, 'failed')
        time.sleep(0.5)
        
if __name__ =='__main__':
    do_tests()
