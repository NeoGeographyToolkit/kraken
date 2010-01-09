#!/usr/bin/env python
import sys, os, uuid, time, traceback
import threading
from subprocess import Popen, PIPE, STDOUT
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))    
import protocols
import protocols.rpc_services
from protocols import protobuf, dotdict

from amqplib import client_0_8 as amqp
from amqplib.client_0_8.basic_message import Message
from messaging.amq_config import connection_params, which
from messaging.messagebus import MessageBus, ConsumptionThread
from threading import Event
#import signal

import logging
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
#logging.getLogger('messagebus').setLevel(logging.DEBUG)

if os.path.dirname(__file__).strip():
    COMMAND_PATH = os.path.join(os.path.dirname(__file__), 'commands')
else:
    COMMAND_PATH = './commands' 
print "command path is %s" % COMMAND_PATH

RPC_RETRIES = 3 # number of times to retry on RPC timeouts

class Reaper(object):

    commands = {
        'test': '../messaging/fake_command.py',
        'test_fjord': '../messaging/fake_command.py',
        'test_bjorn': '../messaging/fake_command.py',
        'moc-stage': os.path.join(COMMAND_PATH, 'moc_stage.py'), # convert and map-project MOC images
        'scale2int8': os.path.join(COMMAND_PATH, 'scale2int8.py'), 
        'mosaic': '/big/software/visionworkbench/bin/image2plate',
        'mipmap': '/big/software/visionworkbench/bin/image2plate',
    }
    JOB_POLL_INTERVAL = 1 #seconds


    REAPER_TYPE = 'generic'
    JOB_EXCHANGE_NAME = 'Job_Exchange'
    CONTROL_EXCHANGE_NAME = 'Control_Exchange'
    STATUS_EXCHANGE_NAME = 'Status_Exchange'


    def __init__(self):
        self.reaper_id = uuid.uuid1().hex
        self.messagebus = MessageBus()
        self.chan = self.messagebus.channel
        self.is_registered = None
        
        self.CONTROL_QUEUE_NAME = "control.reaper.%s" % self.reaper_id
        self.REPLY_QUEUE_NAME = "reply.reaper.%s" % self.reaper_id # queue for RPC responses
        self.JOB_QUEUE_NAME = "reaper."+ self.REAPER_TYPE
        self.logger = logging.getLogger('reaper.%s' % self.reaper_id)
        self.logger.setLevel(logging.DEBUG)
        

        # Consume from the job queue...
        self.chan.exchange_declare(self.JOB_EXCHANGE_NAME, type="direct", durable=True, auto_delete=False)
        self.chan.queue_declare(queue=self.JOB_QUEUE_NAME, durable=True, exclusive=False, auto_delete=False)
        self.chan.queue_bind(queue=self.JOB_QUEUE_NAME, exchange=self.JOB_EXCHANGE_NAME, routing_key=self.JOB_QUEUE_NAME)

        # Publish to the status exchange.
        self.chan.exchange_declare(exchange=self.STATUS_EXCHANGE_NAME, type="fanout", durable=True, auto_delete=False)

        # Notify the dispatcher and accept control commands via the control exchange:
        self.chan.exchange_declare(self.CONTROL_EXCHANGE_NAME, type='direct')
        self.chan.queue_declare(queue=self.CONTROL_QUEUE_NAME, durable=False, auto_delete=True)
        self.chan.queue_bind(queue=self.CONTROL_QUEUE_NAME, exchange=self.CONTROL_EXCHANGE_NAME, routing_key=self.CONTROL_QUEUE_NAME)
        #self.chan.queue_bind(queue=self.CONTROL_QUEUE_NAME, exchange=self.CONTROL_EXCHANGE_NAME, routing_key="control.reaper")
        
        # RPC Service to dispatch
        self.chan.queue_declare(queue=self.REPLY_QUEUE_NAME, durable=False, auto_delete=True)
        self.chan.queue_bind(self.REPLY_QUEUE_NAME, self.CONTROL_EXCHANGE_NAME, routing_key=self.REPLY_QUEUE_NAME)
        self.dispatch_rpc_channel = protocols.rpc_services.RpcChannel(self.CONTROL_EXCHANGE_NAME, self.REPLY_QUEUE_NAME, 'dispatch', max_retries=3)
        self.dispatch = protobuf.DispatchCommandService_Stub(self.dispatch_rpc_channel)
        self.amqp_rpc_controller = protocols.rpc_services.AmqpRpcController(timeout_ms=10000)
        

        # Init threads to handle message consumption
        self.shutdown_event = threading.Event()
        self.control_listener = ConsumptionThread(mode='GET', shutdown_event=self.shutdown_event, name="control_listener")
        #self.job_listener = ConsumptionThread(mode='GET', shutdown_event=self.shutdown_event, name="job_listener")

    
    def send_job_status(self, uuid, status, output=None):
        """ Issue a message to the status bus requesting to update a job's status."""
        args = {'job_id':uuid, 'state':status, 'reaper_id': self.reaper_id}
        if output != None:
            args['output'] = output
        msg_body = protocols.pack(protobuf.JobStatus, args)
        retries = 0
        while retries < 5:
            try:
                self.chan.basic_publish( Message(msg_body), exchange=self.STATUS_EXCHANGE_NAME, routing_key='.'.join((self.REAPER_TYPE, 'job')) )
                break
            except OSError as e:
                logger.error(str(e))
                if e.errno == 32 and retries < 4:
                    if retries > 0:
                        logger.exception("Status publish failed more than once! (Errno 32, Broken Pipe)")
                    retries += 1
                    logger.error("Retrying.")
                    continue
                else:
                    raise
        self.logger.debug("Sent status %s to %s" % (msg_body, self.STATUS_EXCHANGE_NAME))
    
    
    ####
    # RPC calls to dispatch
    ####
    
    def _rpc_failure(self):
        '''Should be called when RPC calls fail (i.e. return value == None)'''
        if self.amqp_rpc_controller.TimedOut():
                self.logger.error("RPC request timed out.")
        elif self.amqp_rpc_controller.Failed():
                self.logger.error("Error in RPC: " + str(self.amqp_rpc_controller.ErrorText()))
        else:
            assert False # If this happens, we're missing a failure state.
        self.amqp_rpc_controller.Reset()
                
    def _get_a_job(self):
        ''' Ask Dispatch for a job and return it.
            If there's no job, return false.
        '''
        request = protobuf.ReaperJobRequest()
        request.reaper_uuid = self.reaper_id
        self.logger.debug("Requesting job.")
        response = self.dispatch.getJob(self.amqp_rpc_controller, request, None)
        if not response:
            self._rpc_failure()
            return None
        elif not response.job_available:
            self.logger.debug("No jobs available.")
            return None
        else:
            self.logger.debug("Got a job: %s" % response.uuid[:8])
            return response
            
    def _report_job_start(self, job, subp, start_time):
        ''' Send back info that's only acessible once the job is running '''
        # note that "job" here is a Protobuf object
        request = protobuf.ReaperJobStartRequest()
        request.job_id = job.uuid
        request.state = 'processing'
        request.reaper_id = self.reaper_id
        request.start_time = start_time.isoformat()
        request.pid = subp.pid
        
        # TODO: Implement retries the right way: in AmqpRpcController (it should have an auto-retry value you set on instantiation)
        retries = 0
        while True:
            response = self.dispatch.jobStarted(self.amqp_rpc_controller, request, None)
            if not response:
                if self.amqp_rpc_controller.TimedOut() and retries <= RPC_RETRIES:
                    retries += 1
                    self.logger.warning("AMQRPC request timed out.  Retrying %d" % retries)
                    self.amqp_rpc_controller.Reset()
                    continue
                else:
                    self._rpc_failure()
                    # TODO: cancel the job?  fail the job? sleep and retry?
                    break 
            self.logger.debug("ACK response: %d" % response.ack)
            if response.ack == protobuf.AckResponse.NOACK: 
                # this is bad.  something happened on the server side.  probably invalid job_id
                self.logger.error("Got Negative ACK trying to report job start.  Something's horribly wrong with dispatch. (job uuid: %s)" % job.uuid)
                # TODO: cancel the job when this happens... or retry?
                raise protocols.rpc_services.SanityError("Got Negative ACK trying to report job start.  Something's horribly wrong with dispatch. (job uuid: %s)" % job.uuid)
            elif response.ack == protobuf.AckResponse.ACK:
                # We're good!
                break
            
    def _report_job_end(self, job, state, end_time, output):
        # note that "job" here is a Protobuf object
        assert end_time
        request = protobuf.ReaperJobEndRequest()
        request.job_id = job.uuid
        request.state = state
        request.end_time = end_time.isoformat()
        request.output = output
        
        # TODO: Implement retries the right way: in AmqpRpcController (it should have an auto-retry value you set on instantiation)
        retries = 0
        while True:
            response = self.dispatch.jobEnded(self.amqp_rpc_controller, request, None)
            if not response:
                if self.amqp_rpc_controller.TimedOut() and retries <= RPC_RETRIES:
                    retries += 1
                    self.logger.warning("AMQRPC request timed out. Retrying (%d)" % retries)
                    self.amqp_rpc_controller.Reset()
                    continue
                else:
                    self._rpc_failure()
                    # TODO: cancel the job?  fail the job? sleep and retry?
                    break  
            if response.ack == protobuf.AckResponse.NOACK: 
                # this is bad.  something happened on the server side.  probably invalid job_id
                self.logger.error("Got Negative ACK trying to report job end.  Something's horribly wrong with dispatch. (job uuid: %s)" % job.uuid)
                # TODO: cancel the job when this happens... or retry?
                raise protocols.rpc_services.SanityError("Got Negative ACK trying to report job end.  Something's horribly wrong with dispatch. (job uuid: %s)" % job.uuid)
            elif response.ack == protobuf.AckResponse.ACK:
                # We're good!
                break
                    
     
    def job_request_loop(self):
        job = None
        while True:
            if self.shutdown_event.is_set():
                break
            if self.is_registered:
                job = self._get_a_job()
            if job:
                if job.command in self.commands:  # only commands allowed by the configuration will be executed
                    #self.send_job_status(job.uuid,  'processing')
                    #msg.channel.basic_ack(msg.delivery_tag)
                    args = [ self.commands[job.command] ] + list(job.args or [])
                    self.logger.info("Executing %s" % ' '.join(args))
                    start_time = datetime.utcnow()
                    p = Popen(args, stdout=PIPE, stderr=STDOUT)
                    self._report_job_start(job, p, start_time) # note that "job" here is a Protobuf object
                    output=""
                    while True:
                        line = p.stdout.readline()
                        if line == '' and p.poll() != None:
                            break
                        output += line
                        sys.stdout.write(line)
                    resultcode = p.wait()
                    end_time = datetime.utcnow()
                    if resultcode == 0:
                        state = 'complete'
                    else:
                        state = 'failed'
                    self.logger.info("Job %s: %s" % (job.uuid[:8], state) )
                    #self.send_job_status(job.uuid, state, output=output)
                    self._report_job_end(job, state, end_time, output)
                else:
                    end_time = datetime.utcnow()
                    self.logger.error("Command: '%s' not found in amq_config's list of valid commands." % job.command)
                    #self.send_job_status(job.uuid, 'failed', output="Command: '%s' not found in the list of valid commands for reaper %s" % (job.command, self.uuid))
                    self._report_job_end(job, 'failed', end_time, "Command: '%s' not found in the list of valid commands for reaper %s" % (job.command, self.reaper_id))
            else:
                time.sleep(self.JOB_POLL_INTERVAL)
            self.logger.debug("Reached end of job loop.")
    
    
    ####
    #  Control Commands
    ####
    
    def _rpc_status(self, msg):
        return protocols.pack(protobuf.ReaperStatusResponse, {'status': 'UP'})
    
    def _rpc_shutdown(self, msg):
        response = protocols.pack(protobuf.ReaperStatusResponse, {'status': 'shutting down'})
        self.shutdown(delay=1)
        return response
    
    CONTROL_COMMAND_MAP = {
        'GetStatus': _rpc_status,
        'Shutdown': _rpc_shutdown
    }
    
    def control_command_handler(self, msg, command_map=CONTROL_COMMAND_MAP):
        """ Unpack a message and process commands 
            Speaks the command protocol.
        """
        self.logger.debug("command_handler got a message.")
        request = protocols.unpack(protobuf.RpcRequestWrapper, msg.body)
        self.logger.debug("command msg contents: %s" % str(request))
        response = dotdict()
        response.sequence_number = request.sequence_number
        if request.method in command_map:
            try:
                response.payload = command_map[request.method].__call__(self, request.payload)
                response.error = False
            except Exception, e:
                #self.logger.error("Error in command '%s': %s %s" % (request.method, str(Exception),  e))
                sys.excepthook(*sys.exc_info())
                #traceback.print_tb(sys.last_traceback)
                response.payload = ''
                response.error = True
                response.error_string = str(e)
        else:
            self.logger.error("Invalid Command: %s" % request.method)
            response.payload = None
            response.error = True
            response.error_text = "Invalid Command: %s" % request.method

        self.control_listener.channel.basic_ack(msg.delivery_tag)
        response_bytes = protocols.pack(protobuf.RpcResponseWrapper, response)
        self.control_listener.channel.basic_publish(Message(response_bytes), routing_key=request.requestor)
        self.logger.debug("Sent response to a control command (%s)" % request.method)

    def register_with_dispatch(self):
        request = protobuf.ReaperRegistrationRequest()
        request.reaper_uuid = self.reaper_id
        request.reaper_type = self.REAPER_TYPE
        response = self.dispatch.registerReaper(self.amqp_rpc_controller, request, None)
        try:
            assert response.ack == 0 # ACK
            self.is_registered = True
            self.logger.info("Registration Acknowledged")
        except:
            assert self.amqp_rpc_controller.TimedOut()
            self.is_registered = False
            self.logger.error("!!REGISTRATION FAILED!!")
            self.shutdown()

    def unregister_with_dispatch(self):
        self.logger.debug("unregister_with_dispatch was called.")
        request = protobuf.ReaperUnregistrationRequest()
        request.reaper_uuid = self.reaper_id
        self.logger.debug("Sending unregistration request.")
        response = self.dispatch.unregisterReaper(self.amqp_rpc_controller, request, None)
        self.logger.debug("unregistration request call finished.")
        try:
            assert response.ack == 0 # ACK
            self.logger.info("Unregistration Acknowledged")
        except:
            self.logger.error( "!! UNREGISTRATION FAILED !!")
        
    def shutdown(self, delay=None):
        self.logger.info("Shutdown initiated.")
        if delay:
            time.sleep(delay)
        if not self.shutdown_event.is_set():
            self.shutdown_event.set()
            self.logger.info("Set shutdown event.")
            if self.is_registered:
                self.logger.info("Unregistering with dispatch.")
                self.unregister_with_dispatch()
            #self.control_listener.join()
            self.job_loop.join()
            del self.amqp_rpc_controller
            del self.dispatch
            del self.dispatch_rpc_channel
            self.chan.queue_delete(queue=self.CONTROL_QUEUE_NAME, if_unused=False, if_empty=False)
            self.chan.queue_delete(queue=self.REPLY_QUEUE_NAME)
            self.chan.connection.close()
            self.chan.close()
#    def _sig_shutdown(self, signum, frame):
#        self.logger.info("Got signal. Shutting down.")
#        self.shutdown()

    def launch(self):
        try:
            self.logger.info("Registering and launching message handlers...")
            #signal.signal(signal.SIGINT, self._sig_shutdown)
            
            self.logger.debug("\tcontrol will consume from %s" % self.CONTROL_QUEUE_NAME)
            self.control_listener.set_callback(queue=self.CONTROL_QUEUE_NAME, no_ack=False, callback=self.control_command_handler)
            
            self.logger.debug("\tjob will consume from %s" % self.JOB_QUEUE_NAME)

            self.logger.debug("Launching consume threads...")
            self.control_listener.start()
            time.sleep(0.25)
            #self.job_listener.start()
            self.job_loop = threading.Thread(name="job_loop", target=self.job_request_loop)
            self.job_loop.start()
            time.sleep(0.25)

            self.logger.info("Registering with dispatch...")
            self.register_with_dispatch()
        except:
            self.shutdown()

        try:
            while not self.shutdown_event.is_set():
                time.sleep(0.1) # keep the thread alive
        except KeyboardInterrupt:
            self.shutdown()
        #self.shutdown()
            
if __name__ == '__main__':
    r = Reaper()
    r.launch()

