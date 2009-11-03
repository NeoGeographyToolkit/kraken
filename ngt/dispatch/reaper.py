#!/usr/bin/env python
import sys, os, uuid
import threading
from subprocess import Popen, PIPE

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))    
import protocols

import logging
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logging.getLogger('messagebus').setLevel(logging.DEBUG)

from amqplib import client_0_8 as amqp
from amqplib.client_0_8.basic_message import Message
from messaging.amq_config import connection_params, which
from messaging.messagebus import MessageBus, ConsumptionThread
from threading import Event

if os.path.dirname(__file__).strip():
    COMMAND_PATH = os.path.join(os.path.dirname(__file__), 'commands')
else:
    COMMAND_PATH = './commands' 
print "command path is %s" % COMMAND_PATH


commands = {
    'echo': which('echo'),
    'grep': which('grep'),
    'ls': which('ls'),
    #'test': os.path.join(os.path.split(__file__)[0], 'fake_mosiac.py'), #a test command that randomly fails
    'test': '/Users/ted/code/alderaan/ngt/messaging/fake_command.py',
    'size': which('du'),
    'moc-stage': os.path.join(COMMAND_PATH, 'moc_stage.py'), # convert and map-project MOC images
}

messagebus = MessageBus()
chan = messagebus.channel

REAPER_TYPE = 'generic'
JOB_EXCHANGE_NAME = 'Job_Exchange'
CONTROL_EXCHANGE_NAME = 'Control_Exchange'
STATUS_EXCHANGE_NAME = 'Status_Exchange'
REAPER_ID = uuid.uuid1().hex
CONTROL_QUEUE_NAME = "control.reaper.%s" % REAPER_ID
JOB_QUEUE_NAME = "reaper."+REAPER_TYPE
logger = logging.getLogger('reaper.%s' % REAPER_ID)


# Consume from the job queue...
chan.exchange_declare(JOB_EXCHANGE_NAME, type="direct", durable=True, auto_delete=False)
chan.queue_declare(queue=JOB_QUEUE_NAME, durable=True, exclusive=False, auto_delete=False)
chan.queue_bind(queue=JOB_QUEUE_NAME, exchange=JOB_EXCHANGE_NAME, routing_key=JOB_QUEUE_NAME)

# Publish to the status exchange.
chan.exchange_declare(exchange=STATUS_EXCHANGE_NAME, type="fanout", durable=True, auto_delete=False)

# Notify the dispatcher and accept control commands via the control exchange:
chan.exchange_declare(CONTROL_EXCHANGE_NAME, type='topic')
chan.queue_declare(queue=CONTROL_QUEUE_NAME, durable=False, auto_delete=True)
chan.queue_bind(queue=CONTROL_QUEUE_NAME, exchange=CONTROL_EXCHANGE_NAME, routing_key=CONTROL_QUEUE_NAME)
chan.queue_bind(queue=CONTROL_QUEUE_NAME, exchange=CONTROL_EXCHANGE_NAME, routing_key="control.reaper")

# Init threads to handle message consumption
shutdown_event = threading.Event()
control_listener = ConsumptionThread(shutdown_event=shutdown_event, name="control_listener")
job_listener = ConsumptionThread(shutdown_event=shutdown_event, name="job_listener")

def recv_callback(msg):
    """ For testing.  Just print the message body"""
    cmd_params = json.loads(msg.body)
    print 'Received: ' + str(cmd_params) + ' from channel #' + str(msg.channel.channel_id)
    
def send_job_status(uuid, status):
    """ Issue a message to the status bus requesting to update a job's status."""
    msg_body = protocols.pack(protocols.Status, {'job_id':uuid, 'state':status, 'reaper_id': REAPER_ID})
    chan.basic_publish( Message(msg_body), exchange=STATUS_EXCHANGE_NAME, routing_key='.'.join((REAPER_TYPE, 'job')) )
    logger.debug("Sent status %s to %s" % (msg_body, STATUS_EXCHANGE_NAME))
    
def job_command_handler(msg):
    cmd = protocols.unpack(protocols.Command, msg.body)
        
    if cmd.command in commands:  # only commands allowed by the configuration will be executed
        send_job_status(cmd.uuid,  REAPER_ID)
        args = [ commands[cmd.command] ] + list(cmd.args)
        logger.debug("Executing %s" % ' '.join(args))
        resultcode = Popen(args).wait()
        if resultcode == 0:
            send_job_status(cmd.uuid, 'complete')
        else:
            send_job_status(cmd.uuid, 'failed')
        msg.channel.basic_ack(msg.delivery_tag)
    else:
        logger.error("Command: '%s' not found in amq_config's list of valid commands." % cmd.command)
# ***
# * Control Commands
# ***

def shutdown_listeners():
    shutdown_event.set()

CONTROL_COMMANDS = {}        
def control_command_handler(msg):
    cmd = protocols.unpack(protocols.Command, msg.body)
    try:
        CONTROL_COMMANDS[cmd.command](cmd.args)
        msg.channel.basic_ack(msg.delivery_tag)
    except:
        raise
    
def command_to_dispatch(command, args):
    serialized_msg = protocols.pack(protocols.Command, {'command':command, 'args':args})
    chan.basic_publish(Message(serialized_msg), exchange=CONTROL_EXCHANGE_NAME, routing_key='dispatch')

def register_with_dispatch():
    command_to_dispatch('register_reaper', [REAPER_ID, REAPER_TYPE])

def unregister_with_dispatch():
    command_to_dispatch('unregister_reaper', [REAPER_ID])

logger.info("Registering and launching message handlers...")
logger.debug("\tcontrol...")
control_listener.channel.basic_consume(queue=CONTROL_QUEUE_NAME, no_ack=False, callback=control_command_handler)
logger.debug("\tjob...")
job_listener.channel.basic_consume(queue=JOB_QUEUE_NAME, no_ack=False, callback=job_command_handler)

logger.debug("Launching consume threads...")
control_listener.start()
job_listener.start()

logger.info("Registering with dispatch...")
register_with_dispatch()
#ctag = chan.basic_consume(queue=REAPER_TYPE, no_ack=True, callback=job_command_handler)
"""
try:
    while True:
        #chan.wait()
        control_listener.join(0.5)
        job_listener.join(0.5)
except e:
    print "Exception: %s" % str(e)
"""
try:
    while True:
        pass
except:
    shutdown_event.set()
    logger.info("Set shutdown event.")
    unregister_with_dispatch()
    chan.connection.close()
    #chan.basic_cancel(ctag)
    chan.close()

