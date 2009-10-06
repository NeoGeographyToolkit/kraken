#!/usr/bin/env python
import sys
from subprocess import Popen, PIPE
try:
    import json
except ImportError:
    import simplejson as json

import logging
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger()

from amqplib import client_0_8 as amqp
from amqplib.client_0_8.basic_message import Message
from amq_config import connection_params, commands
from messagebus import MessageBus

messagebus = MessageBus()
chan = messagebus.channel

EXCHANGE_NAME = 'ngt.direct'

messagebus.setup_direct_queue('command', exchange=EXCHANGE_NAME, chan=chan)

def recv_callback(msg):
    """ For testing... """
    cmd_params = json.loads(msg.body)
    print 'Received: ' + str(cmd_params) + ' from channel #' + str(msg.channel.channel_id)
    
def send_status(uuid, status):
    """ Issue a message to the status bus requesting to update a job's status."""
    chan.basic_publish( Message('{"uuid": "%s", "status": "%s"}' % (uuid, status)), exchange=EXCHANGE_NAME, routing_key='status' )
    
def execute_command(msg):
    """
    Executes a command based on the JSON contents of a message body.
    Sample JSON payload:
    {
        "uuid": "a35cb06a5525dd319a1d0f298a281602",
        "command": "test",
        "args": ["/store/source/hirise/PDS/PDS/RDR/PSP/ORB_008900_008999/PSP_008936_1810/PSP_008936_1810_COLOR.JP2"] 
    }
    """
    if not msg:
        logger.error("execute_command() called with no message.")
        return None #queue is empty?

    try:
        cmd = json.loads(msg.body)
    except ValueError:
        logger.error('Can not parse JSON from: %s' % msg.body)
        raise
        
    if cmd['command'] in commands:  # only commands allowed by the configuration will be executed
        args = [ commands[cmd['command']] ] + cmd['args']
        logger.debug("Executing %s" % ' '.join(args))
        resultcode = Popen(args).wait()
        if resultcode == 0:
            #report success
            send_status(cmd['uuid'], 'complete')
        else:
            #report failure
            send_status(cmd['uuid'], 'failed')
    else:
        logger.error("Command: '%s' not found in amq_config's list of valid commands." % cmd['command'])
    

ctag = chan.basic_consume(queue='command', no_ack=True, callback=execute_command)

while True:
    chan.wait()
chan.basic_cancel(ctag)


chan.close()
