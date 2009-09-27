#/usr/bin/env python2.6
import sys
sys.path.insert(0, '../..')
from django.core.management import setup_environ
from pds import settings
setup_environ(settings)    

from ngt.messaging.queue import messagebus
from ngt.jobs.models import Job

def process_status_msg(msg):
    import pdb; pdb.set_trace()
    #TODO: update the job in the database
    messagebus.ack(msg.delivery_info['delivery_tag'])
    
ctag = messagebus.register_consumer('status', process_status_msg)

try:
    while True:
        messagebus._chan.wait()
finally:
    messagebus._chan.basic_cancel(ctag)
    
