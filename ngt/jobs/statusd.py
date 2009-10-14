#!/usr/bin/env python
import sys
import atexit
import uuid
import threading
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
logger = logging.getLogger('statusd')
logger.setLevel(logging.DEBUG)

from ngt.messaging.messagebus import MessageBus
from ngt.jobs.models import Job
from ngt import protocols

instance_id = uuid.uuid1().hex
messagebus = MessageBus()

def update_status(pb_string):
    '''
    Update the status of a job based on data from a serialized protocol buffer binary string.
    '''
    stat_msg = protocols.unpack(protocols.Status, pb_string)
    logger.debug("Setting status of job %s to '%s'." % (stat_msg.uuid, stat_msg.state))
    try:
        job = Job.objects.get(uuid=stat_msg.uuid)
        job.status = stat_msg.state
        job.save()
    except Job.DoesNotExist:
        logger.error("Couldn't find a job with uuid %s on status update." % stat_msg.uuid)
    

def process_status_msg(msg):
    logger.debug("GOT STATUS: %s" % msg.body)
    update_status(msg.body)
    messagebus.ack(msg.delivery_info['delivery_tag'])

def cleanup():
    logger.info("Deleting queue %s" % queuename)
    messagebus.queue_delete(queuename, if_empty=True)
atexit.register(cleanup)

def consume(ctag, shutdown_event):
    while not shutdown_event.is_set():
        messagebus._chan.wait()
        

logger.info("statusd running with instance id %s" % instance_id)
queuename = "status_statusd_%s" % instance_id
messagebus.exchange_declare(exchange="Status_Exchange", type="fanout", durable=True)
messagebus.queue_declare(queue=queuename, durable=True, exclusive=False, auto_delete=False)
messagebus.queue_bind(queue=queuename, exchange='Status_Exchange', routing_key=queuename)
#ctag = messagebus.register_consumer(queuename, process_status_msg, exchange="Status_Exchange")
ctag = messagebus.basic_consume(callback=process_status_msg, queue=queuename)

shutdown_event = threading.Event()
consume_loop = threading.Thread(target=consume, args=(ctag,shutdown_event))
consume_loop.daemon = True

consume_loop.start()
logger.debug("Consuming with ctag %s" % ctag)
while True:
    try:
        consume_loop.join(0.5)
    except KeyboardInterrupt:
        shutdown_event.set()
        messagebus._chan.basic_cancel(ctag)
        #logger.debug("Consumption cancelled.")
        sys.exit(0)