import sys, os, time, logging

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))    
import protocols
import protocols.rpc_services
from protocols import protobuf

from amqplib import client_0_8 as amqp
from amqplib.client_0_8.basic_message import Message
from messaging.amq_config import connection_params, which
from messaging.messagebus import MessageBus, ConsumptionThread

sys.path.insert(0, '../..')
from django.core.management import setup_environ
from ngt import settings
setup_environ(settings)
from models import Reaper
from django.db.models import Q

logger = logging.getLogger('reaperctl')
#logger.set_defaults()
logger.setLevel(logging.INFO)


class RpcRig(object):
    """ RpcRig encapsulates the objects necessary make RPCs
        to a single reaper.  Service and controller are the properties you need to make a call:
        response = rpc_rig.service.{methodname}(rpc_rig.controller, request, callback)
    """
    REPLY_QUEUE_NAME = 'reaperctl.reply'
    CONTROL_EXCHANGE_NAME = 'Control_Exchange'
    
    def __init__(self, target_uuid, timeout_ms=1000):
        self.messagebus = MessageBus()
        self.TARGET_QUEUE = "control.reaper.%s" % target_uuid
        self.messagebus.queue_declare(queue=self.REPLY_QUEUE_NAME, durable=False, auto_delete=True)
        self.messagebus.queue_bind(self.REPLY_QUEUE_NAME, self.CONTROL_EXCHANGE_NAME, routing_key=self.REPLY_QUEUE_NAME)
        self.rpc_channel = protocols.rpc_services.RpcChannel(self.CONTROL_EXCHANGE_NAME, self.REPLY_QUEUE_NAME, self.TARGET_QUEUE)
        self.service = protobuf.ReaperCommandService_Stub(self.rpc_channel)
        self.controller = protocols.rpc_services.AmqpRpcController(timeout_ms=timeout_ms)
        
    def __getattr__(self, name):
        ''' Delegate object access to the service stub '''
        if hasattr(self, 'service'):
            return object.__getattribute__(self.service, name)

def active_reapers():
    return Reaper.objects.filter(deleted=False, expired=False).order_by('status')

def find_reaper(partial_uuid):
    r = Reaper.objects.filter(uuid__istartswith=partial_uuid)
    if r.count() > 1:
        raise Exception, "Given uuid matches more than one reaper."
    return r[0]
        
def list_reapers():
    reapers = active_reapers()
    for r in reapers:
        print "%s\t%s\t%s" % (r.status, r.uuid, r.jobcount)
        
def shutdown_reaper(uuid):
    #reaper = Reaper.objects.get(uuid=uuid)
    logger.info("Requesting shutdown for reaper %s" % uuid[0:8])
    rig = RpcRig(uuid)
    request = protobuf.ReaperShutdownRequest()
    response = rig.service.Shutdown(rig.controller, request, None)
    assert response.status == 'shutting down'
    logger.info("Got shutdown ACK.")
    #reaper.status = response.status
    #reaper.save()
    
def shutdown_all():
    reapers = active_reapers()
    for r in reapers:
        logger.info("Shutting down reaper %s" % r.uuid[:8])
        shutdown_reaper(r.uuid)
        
def rpc_status(uuid):
    ''' Query a reaper for its status, and set it to unreachable if the request times out. '''
    logger.info("Requesting status for reaper %s" % uuid[0:8])
    reaper = Reaper.objects.get(uuid=uuid)
    rig = RpcRig(uuid)
    request = protobuf.ReaperStatusRequest()
    response = rig.service.GetStatus(rig.controller, request, None)
    if response:
        logger.info("Reaper %s returned status %s" % (uuid[0:8], response.status))
        # update record
        reaper.status = response.status
        reaper.save()
    else:
        assert rig.controller.TimedOut()
        logger.error("Request timed out")
        reaper.status = 'unreachable'
        reaper.save()
    return reaper.status

def poll_all_status():
    reapers = active_reapers()
    if reapers.count() < 1:
        print "No reapers running."
    for r in reapers:
        rpc_status(r.uuid)

def expire_unreachable():
    reapers = Reaper.objects.filter(status='unreachable')
    for r in reapers:
        print "Giving %s one more chance." % r.uuid[0:8]
        status = rpc_status(r.uuid)
        if status == 'unreachable':
            print "Expiring %s" % r.uuid[0:8]
            r.expired = True
            r.status = 'expired'
            r.save()