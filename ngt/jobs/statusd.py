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

messagebus = MessageBus()

def update_status(msg_body):
    '''
    Expects a json string message body and updates a Job's status.
    json schema:
    {
        "uuid": "someuuid",
        "status": "status-to-set"
    }
    '''
    try:
        bdy = json.loads(msg_body)
    except ValueError:
        logger.error('statusd: Can''t parse JSON in "%s"' % bdy)
        return
    logger.debug("Setting status of job %s to '%s'." % (bdy['uuid'], bdy['status']))
    try:
        job = Job.objects.get(uuid=bdy['uuid'])
        job.status = bdy['status']
        job.save()
    except Job.DoesNotExist:
        logger.error("Couldn't find a job with uuid %s on status update." % bdy['uuid'])
    
    

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
    
