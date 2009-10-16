import sys, logging, threading, os, atexit
logger = logging.getLogger('dispatch')
logger.setLevel(logging.DEBUG)
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))    
from ngt import protocols
from ngt.messaging.messagebus import MessageBus
from amqplib.client_0_8 import Message
from ngt.jobs.statusd import update_status
mb = MessageBus()

sys.path.insert(0, '../..')
from django.core.management import setup_environ
from ngt import settings
setup_environ(settings)
from models import Reaper

commands = ['register_reaper', 'unregister_reaper']
command_map = dict([(name, name) for name in commands ])
command_map.update({'shutdown': '_shutdown'})


def register_reaper(args):
    assert len(args) == 1
    reaper_uuid = args[0]
    try:
        Reaper.objects.get(uuid=reaper_uuid)
    except Reaper.DoesNotExist:
        r = Reaper(uuid=reaper_uuid)
        r.save()
        logger.info("Registered reaper: %s" % reaper_uuid)

def unregister_reaper(args):
    assert len(args) == 1
    reaper_uuid = args[0]
    try:
        r = Reaper.objects.get(uuid=reaper_uuid)
        r.delete()
        logger.info("Reaper deleted: %s" % reaper_uuid)
    except Reaper.DoesNotExist:
        logger.error("Tried to delete an unregistered reaper, UUID %s" % reaper_uuid)
    
def command_handler(msg):
    """ Unpack a message and process commands 
        Speaks the command protocol.
    """
    cmd = protocols.unpack(protocols.Command, msg.body)
    logger.debug("command_handler got a message: %s" % str(cmd))
    if cmd.command in command_map:
        #getattr(__module__, command_map[cmd.command])(cmd.args)
        globals()[command_map[cmd.command]](cmd.args)
    else:
        logger.error("Invalid Command: %s" % cmd.command)
    mb.basic_ack(msg.delivery_tag)
        
def status_handler(msg):
    logger.debug("GOT STATUS: %s" % msg.body)
    if update_status(msg.body):
        mb.basic_ack(msg.delivery_info['delivery_tag'])
    else:
        logger.error("STATUS MESSAGE FAILED: %s" % msg.body)
    
def consume_loop(mb, shutdown_event):
    logger.debug("Starting dispatch consume loop.")
    while mb.channel.callbacks and not shutdown_event.is_set():
        mb.wait()
    logger.debug("dispatch consume loop terminating.")
        

def _shutdown(*args):
    shutdown_event.set()
    
def shutdown():
    payload = protocols.pack(protocols.Command, {'command':'shutdown'})
    print "sending ", payload
    mb.channel.basic_publish(Message(payload), exchange='Control_Exchange', routing_key='dispatch')
    
def init():
    global command_ctag, status_ctag, thread_consume_loop, shutdown_event
    
    atexit.register(shutdown)
    
    #setup command queue
    mb.exchange_declare('Control_Exchange', type='topic')
    mb.queue_declare('control.dispatch')
    mb.queue_bind(queue='control.dispatch', exchange='Control_Exchange', routing_key='dispatch')
    command_ctag = mb.basic_consume(callback=command_handler, queue='control.dispatch')

    #setup status queue
    mb.exchange_declare('Status_Exchange', type='fanout')
    mb.queue_declare('status.dispatch')
    mb.queue_bind(queue='status.dispatch', exchange='Status_Exchange', routing_key='dispatch')
    status_ctag = mb.basic_consume(callback=status_handler, queue='status.dispatch')

    shutdown_event = threading.Event()
    thread_consume_loop = threading.Thread(target=consume_loop, args=(mb, shutdown_event) )
    thread_consume_loop.daemon = True
    thread_consume_loop.start()

if __name__ == '__main__':
    init()
