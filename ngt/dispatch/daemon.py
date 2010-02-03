#!/usr/bin/env python2.6
import sys, logging, threading, os, atexit, time, optparse
from datetime import datetime
import itertools, traceback, json
import Queue
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

#from ngt.protocols import * # <-- dotdict, pack, unpack
import ngt.protocols as protocols
from ngt.protocols import protobuf, dotdict, rpc_services
from ngt.utils.containers import UniquePriorityQueue

from ngt.messaging import messagebus
from ngt.messaging.messagebus import MessageBus
from amqplib.client_0_8 import Message
logger = logging.getLogger('dispatch')
logger.setLevel(logging.INFO)
d_logger = logging.getLogger('dispatch_debug')
#d_logger.addHandler(logging.FileHandler('dispatch.log', 'w') )
#d_logger.setLevel(logging.DEBUG)
#logging.getLogger().setLevel(logging.DEBUG)
#logging.getLogger('protocol').setLevel(logging.DEBUG)



mb = MessageBus()

sys.path.insert(0, '../..')
from django.core.management import setup_environ
from ngt import settings
setup_environ(settings)
from models import Reaper
from ngt.jobs.models import Job, JobSet
from django import db
from django.db.models import Q
from commands import jobcommands


command_map = {
    'registerReaper': 'register_reaper',
    'unregisterReaper': 'unregister_reaper',
    'getJob': 'get_next_job',
    'jobStarted': 'job_started',
    'jobEnded': 'job_ended',
    'shutdown': '_shutdown',
}


JOB_FETCH_LIMIT = 30   
REFRESH_TRIGGER_SIZE = 3

class NoopLock(object):
    def acquire(self): pass
    def release(self): pass
#dblock = threading.RLock()
dblock = NoopLock()

def create_jobcommand_map():
    ''' Create a map of command names to JobCommand subclasses from the jobcommand module '''
    jobcommand_map = {}
    # [type(getattr(jobcommands,o)) == type and issubclass(getattr(jobcommands,o), jobcommands.JobCommand) for o in dir(jobcommands)]
    for name in dir(jobcommands):
        obj = getattr(jobcommands, name)
        if type(obj) == type and issubclass(obj, jobcommands.JobCommand):
            if obj.name in jobcommand_map:
                raise ValueError("Duplicate jobcommand name: %s" % obj.name)
            jobcommand_map[obj.name] = obj
    return jobcommand_map
jobcommand_map = create_jobcommand_map()
logger.debug("jobcommand_map initialized: %s" % str(jobcommand_map))
logger.debug("Valid jobcommands:")
for k in jobcommand_map.keys():
    logger.debug(k)
    

####
# Database Thread
###
class TaskQueueThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        super(TaskQueueThread, self).__init__(*args, **kwargs)
        self.daemon = True
        #self.task_queue = Queue.PriorityQueue()
        self.task_queue = Queue.Queue()
        self.default_priority = 3
        
    def enqueue(self, method, *args, **kwargs):
        priority = kwargs.pop('priority', self.default_priority)
        #logger.debug("%s is enqueueing a %s task with priority %d." % (self.name, method.__name__, priority) )
        #logger.debug("ARGS: %s :: %s" % ( str(args) , str(kwargs) ) )
        self.task_queue.put( (priority, method, args, kwargs) )
        
    def run(self):
        logger.debug("%s is running." % self.name)
        while True:
            priority, method, args, kwargs = self.task_queue.get()
            #logger.debug("%s is executing a %s task" % (self.name, method.__name__))
            #logger.debug("ARGS: %s :: %s" % ( str(args) , str(kwargs) ) )
            method(*args, **kwargs)
            self.task_queue.task_done()


###
# COMMANDS
###

def _save_job(job):
    dblock.acquire()
    job.save()
    dblock.release()

def _register_reaper(request):
    dblock.acquire()
    try:
        r = Reaper.objects.get(uuid=request.reaper_uuid) # will get deleted or expired reapers, too
        logger.info("Reaper %s exists.  Resurrecting." % request.reaper_uuid[:8])
        if 'hostname' in request:
            r.hostname = request.hostname
        r.deleted = False
        r.expired = False
        r.save()
    except Reaper.DoesNotExist:
        r = Reaper(uuid=request.reaper_uuid, type=request.reaper_type)
        if 'hostname' in request:
            r.hostname = request.hostname
        r.save()
        logger.info("Registered reaper: %s" % request.reaper_uuid)
    finally:
        dblock.release()

def register_reaper(msgbytes):
    # TODO: Handle the corner case where a reaper has been expired or soft-deleted, and tries to register itself again.
    # Currently this would result in a ProgrammerError from psycopg
    request = protocols.unpack(protobuf.ReaperRegistrationRequest, msgbytes)
    
    logger.info("Got registration request from reaper %s" % request.reaper_uuid)
    thread_database.enqueue(_register_reaper, request)
    return protocols.pack(protobuf.AckResponse, {'ack': protobuf.AckResponse.ACK})

def _unregister_reaper(request):
    dblock.acquire()
    try:
        r = Reaper.objects.get(uuid=request.reaper_uuid)
        r.soft_delete()
        logger.info("Reaper deleted: %s" % request.reaper_uuid)
    except Reaper.DoesNotExist:
        logger.error("Tried to delete an unregistered reaper, UUID %s" % reaper_uuid)
    finally:
        dblock.release()

def unregister_reaper(msgbytes):
    request = protocols.unpack(protobuf.ReaperUnregistrationRequest, msgbytes)
    thread_database.enqueue(_unregister_reaper, request)
    return protocols.pack(protobuf.AckResponse, {'ack': protobuf.AckResponse.ACK})

####
# Job Fetching Logic
####



def check_readiness(job):
    '''Return True if the job is ready to be processed, False otherwise.'''
    if not job.dependencies_met():
        logger.debug("Job %s(%s) has unmet dependencies." % (job.uuid[:8], job.command)) 
        return False
    if job.command in jobcommand_map:
        return jobcommand_map[job.command].check_readiness(job)
    else:
        return True
    
def preprocess_job(job):
    ''' Anything that needs to get done before the job is dispatched '''
    if job.command in jobcommand_map:
        return jobcommand_map[job.command].preprocess_job(job)
    else:
        return job

def postprocess_job(job):
    ''' Anything that needs to get done after the job is completed '''
    if job.command in jobcommand_map:
        return jobcommand_map[job.command].postprocess_job(job)
    else:
        logger.debug("Skipping postprocessing because the job's command is not in jobcommand_map.")
        return job


class JobBuffer(UniquePriorityQueue):

    def __init__(self, maxsize):
        UniquePriorityQueue.__init__(self, maxsize)
        self.refreshing = False
        self.refresh()

    def _refresh(self):
        statuses_to_fetch = (Job.StatusEnum.NEW, Job.StatusEnum.REQUEUE)
        logger.debug("Refreshing the job buffer.")
        dblock.acquire()
        t0 = time.time()
        rejected_count = 0
        for jobset in JobSet.objects.filter(active=True).order_by('priority'):
            jobs = jobset.jobs.filter(status_enum__in=statuses_to_fetch).order_by('transaction_id','id')[:JOB_FETCH_LIMIT]
            for job in jobs:
                if check_readiness(job):
                    job.status_enum = Job.StatusEnum.DISPATCHED
                    job.save()
                    self.put((jobset.priority, job))
                else:
                    rejected_count += 1
                    logger.debug("REFRESH: %s rejected because it's not ready to run." % str(job))

        #db.connection.close() # force django to close connections, otherwise it won't
        dblock.release()
        d_logger.debug("Refresh complete in %f secs.  New size: %d Rejected: %d" % (time.time() - t0, self.qsize(), rejected_count) )
        self.refreshing = False

    def refresh(self):
        if not self.refreshing:
            self.refreshing = True
            thread_database.enqueue(self._refresh)
            
    def next(self):
        logger.debug("%d jobs in buffer. Refreshing: %s" % (self.qsize(), str(self.refreshing)))
        if self.qsize() <= REFRESH_TRIGGER_SIZE:
            logger.debug("Job buffer low.  Requesting refresh.")
            self.refresh()
        priority, job = self.get(False) # False means non-blocking
        return job

    
def get_next_job(msgbytes):
    global dispatched_job_count
    t0 = time.time()
    logger.debug("Looking for the next job.")
    request = protocols.unpack(protobuf.ReaperJobRequest, msgbytes)
    
    try:
        job = job_buffer.next()
    except Queue.Empty:
        job = None
        logger.info("Job buffer empty.")
        return protocols.pack(protobuf.ReaperJobResponse,{'job_available': False})
    assert job

    t1 = time.time()
    logger.debug("Fetched a job in %s sec." % str(t1-t0))
            
    job = preprocess_job(job)
    response = {
        'job_available' : True,
        'uuid' : job.uuid,
        'command' : job.command,
        'args' : json.loads(job.arguments or '[]'),
        }
    logger.info("Sending job %s to reaper %s (%s)" % (job.uuid[:8], request.reaper_uuid[:8], str(time.time() - t0)))
    job.status = "dispatched"
    job.processor = request.reaper_uuid
    #dblock.acquire()
    #job.save()
    thread_database.enqueue(_save_job, job)
    job = None
    #dblock.release()
    if options.show_queries:
        # print the slowest queries
        from django.db import connection
        from pprint import pprint
        pprint([q for q in connection.queries if float(q['time']) > 0.001])
    dispatched_job_count += 1
    d_logger.debug("Dispatching %s" % response['uuid'][:8])
    return protocols.pack(protobuf.ReaperJobResponse, response)
    
####
# Job Status Updates
###

def job_does_not_exist(uuid):
    logger.warning("Got a status update about a nonexistant job.  UUID: %s" % uuid)
    
def reaper_does_not_exist(reaper_uuid):
    # <shrug> Log a warning, register the reaper
   logger.warning("Dispatch received a status message from unregistered reaper %s.  Reaper record will be created." % request['reaper_id'])
   register_reaper(protocols.pack(protobuf.ReaperRegistrationRequest, {'reaper_uuid':reaper_uuid, 'reaper_type':'generic'}))

def verify_reaper_id(job_uuid, reported_uuid, recorded_uuid):
    ''' Warn if a status message comes back from a reaper other than the one we expect for a given job. '''
    try:
        assert reported_uuid == recorded_uuid
    except AssertionError:
        tup = (job_uuid[:8], reported_uuid[:8], recorded_uuid[:8])
        logger.warning("Job %s expected to be handled by Reaper %s, but a status message came from reaper %s.  Probably not good." % tup )

def _job_started(request):
    '''Update the Job record to with properties defined at job start (pid, start_time,...)'''
    logger.debug("Setting job %s to processing." % request.job_id[:8])
    d_logger.debug("Commit start: %s" % request.job_id[:8])
    try:
        dblock.acquire()
        job = Job.objects.get(uuid=request.job_id)
        logger.debug("Got job %s from DB." % job.uuid[:8])
    except Job.DoesNotExist:
        job_does_not_exist(request.job_id)
        raise
    verify_reaper_id(request.job_id, request.reaper_id, job.processor)
    
    # assert job.status == 'dispatched'
    job.time_started = request.start_time.replace('T',' ') # django DateTimeField should be able to parse it this way. (pyiso8601 would be the alternative).
    job.pid = request.pid
    job.status = request.state or 'processing'

    # get reaper & set current job...
    job.save()
    logger.debug("Job %s saved" % job.uuid[:8])
    """
    ####
    # Reaper current job tracking is disabled because it is of dubious usefulness 
    # and makes it harder to reset certain JobSets that require Job deletion.
    ####
    
    try:
        reaper = Reaper.objects.get(uuid=request.reaper_id)
    except Reaper.DoesNotExist:
        # <shrug> Log a warning, register the reaper
        logger.warning("Dispatch received a status message from unregistered reaper %s.  Probably not good." % request['reaper_id'])
        register_reaper(protocols.pack(protobuf.ReaperRegistrationRequest, {'reaper_uuid':request.reaper_id, 'reaper_type':'generic'}))
        reaper = Reaper.objects.get(uuid=request.reaper_id)
    reaper.current_job = job    
    reaper.save()
    logger.debug("Reaper %s saved" % reaper.uuid[:8])
    """
    dblock.release()
    
def job_started(msgbytes):
    '''Update the Job record to with properties defined at job start (pid, start_time,...)'''
    request = protocols.unpack(protobuf.ReaperJobStartRequest, msgbytes)
    logger.debug("Received job start message: %s" % str(request))
    d_logger.debug("Request start: %s" % request.job_id[:8])
    
    # add request to the database queue
    thread_database.enqueue(_job_started, request)
    
    resp = {'ack': protobuf.AckResponse.ACK}
    logger.debug("Response to send: " + str(resp))
    return protocols.pack(protobuf.AckResponse, resp)

def _job_ended(request):
    logger.info("Setting job %s to %s" % (request.job_id[:8], request.state))
    d_logger.debug("Commit end: %s (%s)" % (request.job_id[:8], request.state))
    dblock.acquire()
    try:
        job = Job.objects.get(uuid=request.job_id)
    except Job.DoesNotExist:
        job_does_not_exist(request.job_id)
        raise
        #return protocols.pack(protobuf.AckResponse, {'ack': protobuf.AckResponse.NOACK})
    
    # assert job.status == 'processing'
    job.status = request.state
    job.time_ended = request.end_time.replace('T',' ') # django DateTimeField should be able to parse it this way. (pyiso8601 would be the alternative).
    job.output = request.output
    job = postprocess_job(job)
    job.save()
    # TODO: get reaper and increment job count
    try:
        reaper = Reaper.objects.get(uuid=job.processor)
        reaper.jobcount += 1
        reaper.current_job_id = None
        reaper.save()
    except Reaper.DoesNotExist:
        # <shrug> Log a warning
        logger.warning("A job ended that was assigned to an unregistered reaper %s.  Probably not good." % request.reaper_id)
        
        
    if request.state == 'complete' and job.creates_new_asset:
        try:
            job.spawn_output_asset()
        except:
            logger.error("ASSET CREATION FAILED FOR JOB %s" % job.uuid)
            sys.excepthook(*sys.exc_info())
            job.status = "asset_creation_fail"
            job.save()
            
    dblock.release()

def job_ended(msgbytes):
    '''Update job record with properties defined at job end time ()'''
    request = protocols.unpack(protobuf.ReaperJobEndRequest, msgbytes)
    logger.info("Job %s ended: %s" % (request.job_id[:8], request.state))
    d_logger.debug("Request end: %s (%s)" % (request.job_id[:8], request.state) )
    #time.sleep(0.10)
    thread_database.enqueue(_job_ended, request)
    
    return protocols.pack(protobuf.AckResponse, {'ack': protobuf.AckResponse.ACK})
    
###
# Handlers
###

def command_handler(msg):
    """ Unpack a message and process commands 
        Speaks the command protocol.
    """
    global command_request_count
    command_request_count += 1
    mb.basic_ack(msg.delivery_tag)
    #cmd = protocols.unpack(protocols.Command, msg.body)
    request = protocols.unpack(protobuf.RpcRequestWrapper, msg.body)
    logger.debug("command_handler got a command: %s" % str(request.method))
    response = dotdict()
    response.sequence_number = request.sequence_number
    
    if request.method in command_map:
        t0 = datetime.now()
        try:
            response.payload = globals()[command_map[request.method]](request.payload)
            response.error = False
        except Exception as e:
            logger.error("Error in command '%s': %s %s" % (request.method, type(e),  str(e.args)))
            # TODO: send a stack trace.
            traceback.print_tb(sys.exc_info()[2]) # traceback
            response.payload = ''
            response.error = True
            response.error_string = str(e)
        t1 = datetime.now()
        logger.info("COMMAND %s finished in %s." % (request.method, str(t1-t0)))
    else:
        logger.error("Invalid Command: %s" % request.method)
        response.payload = None
        response.error = True
        response.error_text = "Invalid Command: %s" % request.method

    #mb.basic_ack(msg.delivery_tag)
    response_bytes = protocols.pack(protobuf.RpcResponseWrapper, response)
    mb.basic_publish(Message(response_bytes), routing_key=request.requestor)
        
    
def consume_loop(mb, shutdown_event):
    logger.debug("Starting dispatch consume loop.")
    while mb.channel.callbacks and not shutdown_event.is_set():
        mb.wait()
    logger.debug("dispatch consume loop terminating.")
        

def _shutdown(*args):
    logger.info("Initiating Shutdown")
    mb.shutdown_event.set()
    time.sleep(2)
    sys.exit(1)
    
def shutdown():
    _shutdown()


    
def init():
    logger.debug("dispatch daemon initializing")
    global command_ctag, status_ctag, thread_consume_loop, thread_database, shutdown_event, job_buffer, dispatched_job_count, command_request_count
    shutdown_event = threading.Event()
    #logging.getLogger('messagebus').setLevel(logging.DEBUG)
    
    logger.info("Resetting previously dispatched jobs.")
    for js in JobSet.objects.filter(active=True):
            js.jobs.filter(status_enum=Job.StatusEnum.DISPATCHED).update(status_enum=Job.StatusEnum.REQUEUE)

    if options.requeue_lost_jobs:
        logger.info("Resetting previously processing jobs.")
        for js in JobSet.objects.filter(active=True):
            js.jobs.filter(status_enum__in=(Job.StatusEnum.PROCESSING,)).update(status_enum=Job.StatusEnum.REQUEUE)
    
    atexit.register(shutdown)
    
    dispatched_job_count = 0
    command_request_count = 0
    thread_database = TaskQueueThread(name="thread_database")
    thread_database.start()
    job_buffer = JobBuffer(0)
    
    # setup command queue
    CONTROL_QUEUE = 'control.dispatch'
    logger.debug("Setting up Command listener")
    mb.exchange_declare('Control_Exchange', type='direct')
    mb.queue_declare(CONTROL_QUEUE,auto_delete=True)
    if not options.restart:
        logger.info ("Purging control queue")
        mb.queue_purge(CONTROL_QUEUE)
    mb.queue_bind(queue=CONTROL_QUEUE, exchange='Control_Exchange', routing_key='dispatch')
    command_ctag = mb.basic_consume(callback=command_handler, queue=CONTROL_QUEUE)

    logger.info("Launching consume thread.")
    mb.start_consuming()
    
def do_report(dt):
    global job_buffer, thread_database, dispatched_job_count, command_request_count
    report = "%f commands/sec\t %f jobs/sec\t %d in job queue\t%d in db queue" % (command_request_count / dt, dispatched_job_count / dt, job_buffer.qsize(), thread_database.task_queue.qsize())
    command_request_count = 0
    dispatched_job_count = 0
    print report
    


if __name__ == '__main__':
    logger.debug("__name__ == '__main__'")
    global options
    parser = optparse.OptionParser()
    parser.add_option('--restart', dest="restart", action='store_true', help="Don't purge the control queue.")
    parser.add_option('--verbose', '--info', dest='loglevel', action='store_const', const=logging.INFO, help='Set log level to INFO.')
    parser.add_option('-v', '--debug', dest='loglevel', action='store_const', const=logging.DEBUG, help='Set log level to DEBUG.')
    parser.add_option('--queries', dest='show_queries', action='store_true', help='Print out the slow queries (django.db.connection.queries)')
    parser.add_option('--lost-jobs', dest='requeue_lost_jobs', action='store_true', help="Requeue jobs marooned with a 'dispatched' or 'processing' status'.")
    parser.add_option('--report-interval', '-i', action='store', type='int', dest='report_interval', help="Report interval in seconds.")
    parser.set_defaults(loglevel=logging.WARNING, report_interval=1)
    (options, args) = parser.parse_args()
    
    logger.setLevel(options.loglevel)
    logger.debug("Starting init()")
    init()
    print "READY."
    try:
        t0 = time.time()
        while True:
            dt = time.time() - t0
            if dt > 1: #one second
                do_report(dt)
                t0 = time.time()
            if not mb.consumption_thread.is_alive():
                logger.error("Consumtion thread died.  Shutting down dispatch.")
                shutdown()
            time.sleep(0.01)
    except KeyboardInterrupt:
        logger.info("Got a keyboard interrupt.  Shutting down dispatch.")
        shutdown()
        sys.exit(0)

