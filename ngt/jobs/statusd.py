#!/usr/bin/env python
import sys
try:
    import json
except ImportError:
    import simplejson as json

sys.path.insert(0, '../..')
from django.core.management import setup_environ
from ngt import settings
setup_environ(settings)

import logging
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger()

from ngt.messaging.messagebus import MessageBus
from ngt.jobs.models import Job
from ngt import protocols

messagebus = MessageBus()

def update_status(pb_string):
    '''
    Update the status of a job based on data from a serialized protocol buffer binary string.
    '''
    stat_msg = protocols.Status()
    stat_msg.ParseFromString(pb_string)
    logger.debug("Setting status of job %s to '%s'." % (stat_msg.uuid, stat_msg.newstatus))
    try:
        job = Job.objects.get(uuid=stat_msg.uuid)
        job.status = stat_msg.newstatus
        job.save()
    except Job.DoesNotExist:
        logger.error("Couldn't find a job with uuid %s on status update." % stat_msg.uuid)
    

def process_status_msg(msg):
    logger.debug("GOT STATUS: %s" % msg.body)
    update_status(msg.body)
    messagebus.ack(msg.delivery_info['delivery_tag'])

ctag = messagebus.register_consumer('status', process_status_msg)

try:
    while True:
        messagebus._chan.wait()
finally:
    messagebus._chan.basic_cancel(ctag)
    
