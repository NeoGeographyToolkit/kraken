#!/usr/bin/env python
import sys, os
from subprocess import Popen, PIPE

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))    
import protocols

import logging
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger()

from amqplib import client_0_8 as amqp
from amqplib.client_0_8.basic_message import Message
from amq_config import connection_params, commands
from messagebus import MessageBus

messagebus = MessageBus()
chan = messagebus.channel

REAPER_TYPE = 'reaper.generic'
COMMAND_EXCHANGE_NAME = 'Command_Exchange'
STATUS_EXCHANGE_NAME = 'Status_Exchange'

# This script consumes from a command queue...
chan.exchange_declare(COMMAND_EXCHANGE_NAME, type="direct", durable=True, auto_delete=False)
chan.queue_declare(queue=REAPER_TYPE, durable=True, exclusive=False, auto_delete=False)
chan.queue_bind(queue=REAPER_TYPE, exchange=COMMAND_EXCHANGE_NAME, routing_key=REAPER_TYPE)
# ...and publishes to the status exchange.
chan.exchange_declare(exchange=STATUS_EXCHANGE_NAME, type="fanout", durable=True, auto_delete=False)

def recv_callback(msg):
    """ For testing.  Just print the message body"""
    cmd_params = json.loads(msg.body)
    print 'Received: ' + str(cmd_params) + ' from channel #' + str(msg.channel.channel_id)
    
def send_status(uuid, status):
    """ Issue a message to the status bus requesting to update a job's status."""
    #stat = protocols.Status()
    #stat.uuid = uuid
    #stat.newstatus = status
    msg_body = protocols.pack(protocols.Status, {'uuid':uuid, 'newstatus':status})
    #chan.basic_publish( Message('{"uuid": "%s", "status": "%s"}' % (uuid, status)), exchange=EXCHANGE_NAME, routing_key='status' )
    chan.basic_publish( Message(msg_body), exchange=STATUS_EXCHANGE_NAME, routing_key='.'.join((REAPER_TYPE, 'job')) )
    
def execute_command(msg):
        
    if not msg:
        # Is the queue empty?
        logger.error("execute_command() called with no message.")
        return None

    #cmd = protocols.Command()
    #cmd.ParseFromString(msg.body)
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
    

ctag = chan.basic_consume(queue=REAPER_TYPE, no_ack=True, callback=execute_command)

while True:
    chan.wait()
chan.basic_cancel(ctag)


chan.close()
