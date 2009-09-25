#!/opt/local/bin/python
from amqplib import client_0_8 as amqp
from subprocess import Popen, PIPE

from amq_config import connection_params, commands
from queue import MessageBus
mbus = MessageBus()
chan = mbus._chan

EXCHANGE_NAME = 'ngt.direct'
    
def log_error(msg):
    #TODO: Real logging
    print msg

#conn = amqp.Connection(**connection_params)
#chan = conn.channel()

#chan.queue_declare(queue="command", durable=True, exclusive=False, auto_delete=False)
#chan.exchange_declare(exchange=EXCHANGE_NAME, type="direct", durable=True, auto_delete=False,)
#chan.queue_bind(queue="command", exchange=EXCHANGE_NAME, routing_key="command")
mbus.setup_direct_queue('command', exchangename=EXCHANGE_NAME)

def recv_callback(msg):
    print 'Received: ' + msg.body + ' from channel #' + str(msg.channel.channel_id)
def execute_command(msg):
    if not msg: 
        return None #queue is empty
    #messages should bein the form "command arg1 arg2 arg3..."
    args = msg.body.split(' ')
    if args[0] in commands:  # only commands allowed by the configuration will be executed
        args[0] = commands[args[0]]
        resultcode = Popen(args).wait()
        if resultcode == 0:
            #report success
            pass
        else:
            #report failure
            pass
    else:
        log_error("Invalid command: %s" % args[0])
    

ctag = chan.basic_consume(queue='command', no_ack=True, callback=execute_command)
while True:
    chan.wait()
chan.basic_cancel(ctag)


chan.close()
conn.close()
