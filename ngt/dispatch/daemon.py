#!/usr/bin/env python
import sys, logging, threading, os, atexit, time
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))    
#from ngt.protocols import * # <-- WireMessage, dotdict, pack, unpack
import ngt.protocols as protocols
from ngt.protocols import protobuf, dotdict
from ngt.protocols.rpc_services import WireMessage
from ngt.messaging.messagebus import MessageBus
from amqplib.client_0_8 import Message

logger = logging.getLogger('dispatch')
logger.setLevel(logging.DEBUG)

mb = MessageBus()

sys.path.insert(0, '../..')
from django.core.management import setup_environ
from ngt import settings
setup_environ(settings)
from models import Reaper
from ngt.jobs.models import Job, JobSet
from django.db.models import Q


command_map = {
    'registerReaper': 'register_reaper',
    'unregisterReaper': 'unregister_reaper',
    'shutdown': '_shutdown',
}


JOB_RELEASE_LIMIT = 5
job_semaphore = threading.Semaphore(JOB_RELEASE_LIMIT)

###
# COMMANDS
###

def register_reaper(msgbytes):
    # TODO: Handle the corner case where a reaper has been expired or soft-deleted, and tries to register itself again.
    # Currently this would result in a ProgrammerError from psycopg
    request = protocols.unpack(protobuf.ReaperRegistrationRequest, msgbytes)
    
    try:
        Reaper.objects.get(uuid=request.reaper_uuid)
    except Reaper.DoesNotExist:
        r = Reaper(uuid=request.reaper_uuid, type=request.reaper_type)
        r.save()
        logger.info("Registered reaper: %s" % request.reaper_uuid)
    return protocols.pack(protobuf.AckResponse, {'ack': protobuf.AckResponse.ACK})

def unregister_reaper(msgbytes):
    request = protocols.unpack(protobuf.ReaperUnregistrationRequest, msgbytes)
    try:
        r = Reaper.objects.get(uuid=request.reaper_uuid)
        r.soft_delete()
        logger.info("Reaper deleted: %s" % request.reaper_uuid)
    except Reaper.DoesNotExist:
        logger.error("Tried to delete an unregistered reaper, UUID %s" % reaper_uuid)
    return protocols.pack(protobuf.AckResponse, {'ack': protobuf.AckResponse.ACK})
        


def update_status(msgbytes):
    '''
    Update the status of a job based on data from a serialized protocol buffer binary string.
    '''
    job_completers = ('complete','failed')
    request = protocols.unpack(protobuf.JobStatus, msgbytes)
    logger.info("Setting status of job %s to '%s'." % (request.job_id, request.state))
    try:
        job = Job.objects.get(uuid=request.job_id)
        job.status = request.state
        if job.processor and job.processor != request.reaper_id:
            logger.warning("Status update for job %s came from a different reaper than we expected.  This jobs processor attribute will be set to the latest reaper that reported." % job.uuid)
        job.processor = request.reaper_id
        if 'output' in request:
            job.output = request.output
        job.save()
        if 'reaper_id' in request and request['reaper_id']:
            try:
                r = Reaper.objects.get(uuid=request.reaper_id)
                if request.state in job_completers:
                    r.jobcount += 1
                    r.save()
            except Reaper.DoesNotExist:
                # <shrug> Log a warning, register the reaper, and increment its jobcount
                logger.warning("Dispatch received a status message from unregistered reaper %s.  Probably not good." % request['reaper_id'])
                register_reaper(protocols.pack(protobuf.ReaperRegistrationRequest, {'reaper_uuid':request.reaper_id, 'reaper_type':'generic'}))
                r = Reaper.objects.get(uuid=request.reaper_id)
                    if request.state in job_completers:
                        r.jobcount += 1
                        r.save()
        if request.state in job_completers:
            job_semaphore.release()
            if job.creates_new_asset:
                job.spawn_output_asset()
        return protocols.pack(protobuf.AckResponse, {'ack': protobuf.AckResponse.ACK})
    except Job.DoesNotExist:
        logger.error("Couldn't find a job with uuid %s on status update." % request.uuid)
        return protocols.pack(protobuf.AckResponse, {'ack': protobuf.AckResponse.NOACK})
    
###
# Handlers
###

def command_handler(msg):
    """ Unpack a message and process commands 
        Speaks the command protocol.
    """
    #cmd = protocols.unpack(protocols.Command, msg.body)
    request = WireMessage.unpack_request(msg.body)
    logger.debug("command_handler got a message: %s" % str(request))
    response = dotdict()
    if request.method in command_map:
        try:
            response.payload = globals()[command_map[request.method]](request.payload)
            response.error = False
        except Exception, e:
            logger.error("Error in command '%s': %s %s" % (request.method, str(Exception),  e))
            response.payload = ''
            response.error = True
            response.error_string = str(e)
    else:
        logger.error("Invalid Command: %s" % request.method)
        response.payload = None
        response.error = True
        response.error_text = "Invalid Command: %s" % request.method

    mb.basic_ack(msg.delivery_tag)
    wireresponse = WireMessage.pack_response(response)
    mb.basic_publish(Message(wireresponse), routing_key=request.requestor)
        
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
    mb.shutdown_event.set()
    time.sleep(2)
    sys.exit(1)
    
def shutdown():
    # payload = protocols.pack(protocols.Command, {'command':'shutdown'})
    # print "sending ", payload
    # mb.channel.basic_publish(Message(payload), exchange='Control_Exchange', routing_key='dispatch')
    _shutdown()


def enqueue_jobs(logger, job_semaphore, shutdown_event):
    ''' Loop and enqueue jobs if the maximum queue size hasn't been exceeded '''
    logger.debug("Job dispatch is launching.")
    Job.objects.filter(status='queued').update(status='requeue')
    while True:
        if shutdown_event.is_set():
            break
        for jobset in JobSet.objects.filter(active=True):
            try:
                job = jobset.jobs.filter(Q(status='new') | Q(status='requeue') )[0]
                job_semaphore.acquire()
                job.enqueue()
                time.sleep(0.5)
            except IndexError:
                logger.info("Ran out of jobs to enqueue. Dispatch thread will exit.")
                return True

    
def init():
    logger.debug("dispatch daemon initializing")
    global command_ctag, status_ctag, thread_consume_loop, shutdown_event
    shutdown_event = threading.Event()
    logging.getLogger('messagebus').setLevel(logging.DEBUG)
    
    atexit.register(shutdown)
    
    # setup command queue
    logger.debug("Setting up Command listener")
    mb.exchange_declare('Control_Exchange', type='direct')
    mb.queue_declare('control.dispatch',auto_delete=True)
    mb.queue_bind(queue='control.dispatch', exchange='Control_Exchange', routing_key='dispatch')
    command_ctag = mb.basic_consume(callback=command_handler, queue='control.dispatch')

    # setup status queue
    logger.debug("Setting up status listener.")
    mb.exchange_declare('Status_Exchange', type='fanout')
    mb.queue_declare('status.dispatch', auto_delete=False)
    mb.queue_bind(queue='status.dispatch', exchange='Status_Exchange', routing_key='dispatch')
    status_ctag = mb.basic_consume(callback=status_handler, queue='status.dispatch')

    logger.debug("Launching consume thread.")
    mb.start_consuming()

    if '--moc' in sys.argv:
        dispatch_thread = threading.Thread(name="job_dispatcher", target=enqueue_jobs, args=(logger, job_semaphore, shutdown_event) )
        dispatch_thread.daemon = True
        dispatch_thread.start()

if __name__ == '__main__':
    init()
    try:
        while True:
            if not mb.consumption_thread.is_alive():
                logger.error("Consumtion thread died.  Shutting down dispatch.")
                self.shutdown()
            time.sleep(0.01)
    except KeyboardInterrupt:
        logger.info("Got a keyboard interrupt.  Shutting down dispatch.")
        shutdown()
        sys.exit(0)
