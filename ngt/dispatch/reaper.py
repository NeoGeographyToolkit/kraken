#!/usr/bin/env python2.6
import sys, os, uuid, time, traceback
import threading
from subprocess import Popen, PIPE, STDOUT
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))    
import protocols
import protocols.rpc_services
from protocols import protobuf, dotdict
from ngt.dispatch.services import DispatchService

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
        'snapshot': '/big/software/visionworkbench/bin/snapshot',
        'start_snapshot': '/big/software/visionworkbench/bin/snapshot',
        'end_snapshot': '/big/software/visionworkbench/bin/snapshot',
#        'mipmap': '/big/scratch/logargs.py image2plate',
#        'snapshot': '/big/scratch/logargs.py snapshot',
#        'start_snapshot': '/big/scratch/logargs.py start_snapshot',
#        'end_snapshot': '/big/scratch/logargs.py end_snapshot',
    }
    JOB_POLL_INTERVAL = 1 #seconds


    REAPER_TYPE = 'generic'
    CONTROL_EXCHANGE_NAME = 'Control_Exchange'


    def __init__(self):
        self.reaper_id = uuid.uuid1().hex
        self.messagebus = MessageBus()
        self.chan = self.messagebus.channel
        self.is_registered = None
        
        self.CONTROL_QUEUE_NAME = "control.reaper.%s" % self.reaper_id
        self.REPLY_QUEUE_NAME = "reply.reaper.%s" % self.reaper_id # queue for RPC responses
        self.logger = logging.getLogger('reaper.%s' % self.reaper_id)
        self.logger.setLevel(logging.DEBUG)
        

        # Accept control commands via the control exchange:
        self.chan.exchange_declare(self.CONTROL_EXCHANGE_NAME, type='direct')
        self.chan.queue_declare(queue=self.CONTROL_QUEUE_NAME, durable=False, auto_delete=True)
        self.chan.queue_bind(queue=self.CONTROL_QUEUE_NAME, exchange=self.CONTROL_EXCHANGE_NAME, routing_key=self.CONTROL_QUEUE_NAME)
        
        # RPC Service to dispatch

        self.dispatch = DispatchService(reply_queue=self.REPLY_QUEUE_NAME, )

        # Init threads to handle message consumption
        self.shutdown_event = threading.Event()
        self.control_listener = ConsumptionThread(mode='GET', shutdown_event=self.shutdown_event, name="control_listener")

                
     
    def job_request_loop(self):
        job = None
        while True:
            if self.shutdown_event.is_set():
                break
            if self.is_registered:
                job = self.dispatch.get_a_job(self.reaper_id)
            if job:
                if job.command in self.commands:  # only commands allowed by the configuration will be executed
                    args = self.commands[job.command].split(' ')  + list(job.args or [])
                    self.logger.debug("ARGS: %s" % str(args))
                    self.logger.info("Executing %s" % ' '.join(args))
                    start_time = datetime.utcnow()
                    p = Popen(args, stdout=PIPE, stderr=STDOUT)
                    self.dispatch.report_job_start(self.reaper_id, job, p.pid, start_time) # note that "job" here is a Protobuf object
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
                    self.dispatch.report_job_end(job, state, end_time, output)
                else:
                    end_time = datetime.utcnow()
                    self.logger.error("Command: '%s' not found in amq_config's list of valid commands." % job.command)
                    self.dispatch.report_job_end(job, 'failed', end_time, "Command: '%s' not found in the list of valid commands for reaper %s" % (job.command, self.reaper_id))
            else:
                time.sleep(self.JOB_POLL_INTERVAL)
            self.logger.debug("Reached end of job loop.")
    
    
    ####
    #  Control Commands
    #  TODO: refactor this to an RpcService subclass
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
        
        if self.dispatch.register_reaper(self.reaper_id, self.REAPER_TYPE):
            self.logger.info("Registration successful")
        else:
            self.logger.error("Registration FAILED.")
            self.shutdown()

    def unregister_with_dispatch(self):
        self.logger.debug("unregister_with_dispatch was called.")
        self.dispatch.unregister_reaper(self.reaper_id)
        
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

            self.logger.debug("Launching consume threads...")
            self.control_listener.start()
            time.sleep(0.25)

            self.job_loop = threading.Thread(name="job_loop", target=self.job_request_loop)
            time.sleep(0.25)

            self.logger.info("Registering with dispatch...")
            self.register_with_dispatch()
            
            self.job_loop.start()
        except:
            #self.shutdown()
            raise

        try:
            while not self.shutdown_event.is_set():
                time.sleep(0.1) # keep the thread alive
        except KeyboardInterrupt:
            self.shutdown()
        #self.shutdown()
            
if __name__ == '__main__':
    r = Reaper()
    r.launch()

