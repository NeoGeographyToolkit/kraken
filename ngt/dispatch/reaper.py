#!/usr/bin/env python
import sys, os, uuid
from subprocess import Popen, PIPE

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))    
import protocols

import logging
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

from amqplib import client_0_8 as amqp
from amqplib.client_0_8.basic_message import Message
from messaging.amq_config import connection_params, commands
from messaging.messagebus import MessageBus

messagebus = MessageBus()
chan = messagebus.channel

REAPER_TYPE = 'reaper.generic'
JOB_EXCHANGE_NAME = 'Job_Exchange'
CONTROL_EXCHANGE_NAME = 'Control_Exchange'
STATUS_EXCHANGE_NAME = 'Status_Exchange'
REAPER_ID = uuid.uuid1().hex
logger = logging.getLogger('reaper.%s' % REAPER_ID)


# Consume from the job queue...
chan.exchange_declare(JOB_EXCHANGE_NAME, type="direct", durable=True, auto_delete=False)
chan.queue_declare(queue=REAPER_TYPE, durable=True, exclusive=False, auto_delete=False)
chan.queue_bind(queue=REAPER_TYPE, exchange=JOB_EXCHANGE_NAME, routing_key=REAPER_TYPE)
# Publish to the status exchange.
chan.exchange_declare(exchange=STATUS_EXCHANGE_NAME, type="fanout", durable=True, auto_delete=False)
# Notify the dispatcher via the control exchange:
chan.exchange_declare(CONTROL_EXCHANGE_NAME, type='topic')

def recv_callback(msg):
    """ For testing.  Just print the message body"""
    cmd_params = json.loads(msg.body)
    print 'Received: ' + str(cmd_params) + ' from channel #' + str(msg.channel.channel_id)
    
def send_status(uuid, status):
    """ Issue a message to the status bus requesting to update a job's status."""
    msg_body = protocols.pack(protocols.Status, {'job_id':uuid, 'state':status, 'reaper_id': REAPER_ID})
    chan.basic_publish( Message(msg_body), exchange=STATUS_EXCHANGE_NAME, routing_key='.'.join((REAPER_TYPE, 'job')) )
    logger.debug("Sent status %s to %s" % (msg_body, STATUS_EXCHANGE_NAME))
    
def command_handler(msg):
        
    if not msg:
        # Is the queue empty?
        logger.error("command_handler() called with no message.")
        return None

    cmd = protocols.unpack(protocols.Command, msg.body)
        
    if cmd.command in commands:  # only commands allowed by the configuration will be executed
        args = [ commands[cmd.command] ] + list(cmd.args)
        logger.debug("Executing %s" % ' '.join(args))
        resultcode = Popen(args).wait()
        if resultcode == 0:
            send_status(cmd.uuid, 'complete')
        else:
            send_status(cmd.uuid, 'failed')
    else:
        logger.error("Command: '%s' not found in amq_config's list of valid commands." % cmd.command)
def register_with_dispatch():
    #register this reaper with dispatch
    registration_command = protocols.pack(protocols.Command, {'command':'register_reaper', 'args':[REAPER_ID]})
    chan.basic_publish(Message(registration_command), exchange=CONTROL_EXCHANGE_NAME, routing_key='dispatch')

def unregister_with_dispatch():
    #register this reaper with dispatch
    registration_command = protocols.pack(protocols.Command, {'command':'unregister_reaper', 'args':[REAPER_ID]})
    chan.basic_publish(Message(registration_command), exchange=CONTROL_EXCHANGE_NAME, routing_key='dispatch')

register_with_dispatch()
ctag = chan.basic_consume(queue=REAPER_TYPE, no_ack=True, callback=command_handler)

try:
    while True:
        chan.wait()
finally:
    unregister_with_dispatch()
    chan.basic_cancel(ctag)
    chan.close()

