from ngt import protocols
from ngt.protocols import rpc_services, protobuf
from ngt.protocols.rpc_services import RPCFailure
import logging

d_logger = logging.getLogger('reaper.debug')
#d_logger.addHandler(logging.FileHandler('reaper.log', 'w'))
#d_logger.setLevel(logging.DEBUG)

class DispatchService(rpc_services.AmqpService, protobuf.DispatchCommandService_Stub):
    
    
    #### IMPLEMENTOR REFERENCE ####
    # AmqpService constructor (* denotes a required argument):
    # def __init__(self, 
    #    pb_service_class=None,
    #    amqp_channel=None,
    #    exchange='Control_Exchange',
    #  * request_routing_key=None,
    #  * reply_queue=None,
    #    timeout_ms=5000,
    #    max_retries=3):
    # 
    ####

    def __init__(self, *args, **kwargs):
        kwargs['request_routing_key'] = 'dispatch'
        #kwargs['timeout_ms'] = -1 # never timout
        kwargs['max_retries'] = -1 # retry infinitely
        super(DispatchService, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger('DispatchService')
        
    
    ####
    # RPC calls to dispatch
    ####
    
                
    def get_a_job(self, reaper_id):
        ''' Ask Dispatch for a job and return it.
            If there's no job, return false.
        '''
        request = protobuf.ReaperJobRequest()
        request.reaper_uuid = reaper_id
        self.logger.info("Requesting job.")
        response = self.getJob(self.amqp_rpc_controller, request, None)
        if not response:    
            self._rpc_failure()
            return None
        elif not response.job_available:
            self.logger.info("No jobs available.")
            return None
        else:
            self.logger.info("Got a job: %s" % response.uuid[:8])
            d_logger.debug("%s dispatched" % response.uuid[:8])
            return response
            
    def report_job_start(self, reaper_id, job, pid, start_time):
        ''' Send back info that's only acessible once the job is running '''
        # note that "job" here is a Protobuf object
        request = protobuf.ReaperJobStartRequest()
        request.job_id = job.uuid
        request.state = 'processing'
        request.reaper_id = reaper_id
        request.start_time = start_time.isoformat()
        request.pid = pid
        
        d_logger.debug("Requesting start: %s" % request.job_id [:8])
        response = self.keep_calling(self.jobStarted, request)

        self.logger.debug("ACK response: %d" % response.ack)
        if response.ack == protobuf.AckResponse.NOACK: 
            # this is bad.  something happened on the server side.  probably invalid job_id
            errorstr = "Got Negative ACK trying to report job start. (job uuid: %s)" % job.uuid
            self.logger.error(errorstr)
            raise protocols.rpc_services.SanityError(errorstr)
            # TODO: cancel the job when this happens... or retry?
        elif response.ack == protobuf.AckResponse.ACK:
            # We're good!
            return True
            
    def report_job_end(self, job, state, end_time, output):
        # note that "job" here is a Protobuf object (not a Job instance)
        assert end_time
        request = protobuf.ReaperJobEndRequest()
        request.job_id = job.uuid
        request.state = state
        request.end_time = end_time.isoformat()
        request.output = output
        
        d_logger.debug("Requesting end: %s" % request.job_id[:8])
        response = self.keep_calling(self.jobEnded, request)

        if response.ack == protobuf.AckResponse.NOACK: 
            # this is bad.  something happened on the server side.  probably invalid job_id
            errorstr = "Got Negative ACK trying to report job end. (job uuid: %s)" % job.uuid
            self.logger.error(errorstr)
            raise protocols.rpc_services.SanityError(errorstr)
            # TODO: cancel the job when this happens... or retry?
        elif response.ack == protobuf.AckResponse.ACK:
            # We're good!
            return True
            
    def register_reaper(self, reaper_id, reaper_type, hostname=None):
        request = protobuf.ReaperRegistrationRequest()
        request.reaper_uuid = reaper_id
        request.reaper_type = reaper_type
        if hostname:
            request.hostname = hostname
        response = self.registerReaper(self.amqp_rpc_controller, request, None)
        try:
            assert response.ack == 0 # ACK
            return True
        except:
            assert self.amqp_rpc_controller.TimedOut()
            return False

    def unregister_reaper(self, reaper_id):
        self.logger.debug("unregister_with_dispatch was called.")
        request = protobuf.ReaperUnregistrationRequest()
        request.reaper_uuid = reaper_id
        self.logger.debug("Sending unregistration request.")
        response = self.unregisterReaper(self.amqp_rpc_controller, request, None)
        self.logger.debug("unregistration request call finished.")
        try:
            assert response.ack == 0 # ACK
            return True
        except:
            return False

class ReaperService(rpc_services.AmqpService, protobuf.ReaperCommandService_Stub):
    
    
    #### IMPLEMENTOR REFERENCE ####
    # AmqpService constructor (* denotes a required argument):
    # def __init__(self, 
    #    pb_service_class=None,
    #    amqp_channel=None,
    #    exchange='Control_Exchange',
    #  * request_routing_key=None,
    #  * reply_queue=None,
    #    timeout_ms=5000,
    #    max_retries=3):
    # 
    ####

    def __init__(self, *args, **kwargs):
        kwargs['request_routing_key'] = 'dispatch'
        #kwargs['timeout_ms'] = -1 # never timout
        kwargs['max_retries'] = -1 # retry infinitely
        super(ReaperService, self).__init__(*args, **kwargs)
        self.logger = logging.getLogger('DispatchService')

    def get_status(self):
        pass
    # etc...
