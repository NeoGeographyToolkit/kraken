import sys, os, time
from datetime import datetime
import google.protobuf
from google.protobuf.service import RpcController as _RpcController, Service
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from ngt import protocols
from ngt.messaging.messagebus import MessageBus, amqp, connection as mb_connection, ConsumptionThread
import logging
logging.basicConfig()
logger = logging.getLogger('amqprpc')
logger.setLevel(logging.INFO)
logger.debug("Testing logger.")

#class SanityError(Exception):
#    ''' Raise this when things that should never happen happen. '''
#    pass

class AmqpRpcController(_RpcController):

  """An RpcController mediates a single method call.

  This RpcController implementation can mediate method calls via amqp.
  """
  
  def __init__(self):
      self.m_failed = False
      self.m_failed_reason = ''
      self.m_timeout_flag = False

  # Client-side methods below

  def Reset(self):
    """Resets the RpcController to its initial state.

    After the RpcController has been reset, it may be reused in
    a new call. Must not be called while an RPC is in progress.
    """
    self.m_failed = False
    self.m_failed_reason = ''
    self.m_timeout_flag = False

  def Failed(self):
    """Returns true if the call failed.

    After a call has finished, returns true if the call failed.  The possible
    reasons for failure depend on the RPC implementation.  Failed() must not
    be called before a call has finished.  If Failed() returns true, the
    contents of the response message are undefined.
    """
    return self.m_failed

  def ErrorText(self):
    """If Failed is true, returns a human-readable description of the error."""
    if self.m_failed:
        return self.m_failed_reason
    else:
        return None
        
  def TimedOut(self):
      return self.m_timeout_flag

  def StartCancel(self):
    """Initiate cancellation.

    Advises the RPC system that the caller desires that the RPC call be
    canceled.  The RPC system may cancel it immediately, may wait awhile and
    then cancel it, or may not even cancel the call at all.  If the call is
    canceled, the "done" callback will still be called and the RpcController
    will indicate that the call failed at that time.
    """
    raise NotImplementedError

  # Server-side methods below

  def SetFailed(self, reason):
    """Sets a failure reason.

    Causes Failed() to return true on the client side.  "reason" will be
    incorporated into the message returned by ErrorText().  If you find
    you need to return machine-readable information about failures, you
    should incorporate it into your response protocol buffer and should
    NOT call SetFailed().
    """
    self.m_failed = True
    self.m_failed_reason = reason

  def SetTimedOut(self):
      self.m_timeout_flag = True
      self.SetFailed("RPC call timed out.")

  def IsCanceled(self):
    """Checks if the client cancelled the RPC.

    If true, indicates that the client canceled the RPC, so the server may
    as well give up on replying to it.  The server should still call the
    final "done" callback.
    """
    raise NotImplementedError

  def NotifyOnCancel(self, callback):
    """Sets a callback to invoke on cancel.

    Asks that the given callback be called when the RPC is canceled.  The
    callback will always be called exactly once.  If the RPC completes without
    being canceled, the callback will be called after completion.  If the RPC
    has already been canceled when NotifyOnCancel() is called, the callback
    will be called immediately.

    NotifyOnCancel() must be called no more than once per request.
    """
    raise NotImplementedError


class RpcChannel(object):

  """Abstract interface for an RPC channel.

  An RpcChannel represents a communication line to a service which can be used
  to call that service's methods.  The service may be running on another
  machine. Normally, you should not use an RpcChannel directly, but instead
  construct a stub {@link Service} wrapping it.  Example:

  Example:
    RpcChannel channel = rpcImpl.Channel("remotehost.example.com:1234")
    RpcController controller = rpcImpl.Controller()
    MyService service = MyService_Stub(channel)
    service.MyMethod(controller, request, callback)
  """
  def __init__(self, exchange, response_queue, request_routing_key, max_retries=0, timeout_ms=5000):
      self.exchange = exchange
      self.response_queue = response_queue
      self.request_routing_key = request_routing_key
      self.messagebus = MessageBus()
      self.max_retries = max_retries # maximum number of times to retry on RPC timeout
      self.timeout_ms = timeout_ms
      
      self.sync_sequence_number = 0
      
      #TODO: Setup exchange & queue
      self.messagebus.exchange_declare(exchange, 'direct')
      #self.messagebus.queue_delete(queue=response_queue) # clear it in case there are backed up messages (EDIT: it *should* autodelete)
      self.messagebus.queue_declare(queue=response_queue, auto_delete=True)
      self.messagebus.queue_purge(response_queue)
      self.messagebus.queue_bind(response_queue, exchange, routing_key=response_queue)
      logger.debug("Response queue '%s' is bound to key '%s' on exchange '%s'" % (response_queue, response_queue, exchange))
      

  def CallMethod(self, method_descriptor, rpc_controller,
                 request, response_class, done):
    """Calls the method identified by the descriptor.

    Call the given method of the remote service.  The signature of this
    procedure looks the same as Service.CallMethod(), but the requirements
    are less strict in one important way:  the request object doesn't have to
    be of any specific class as long as its descriptor is method.input_type.
    """
    rpc_controller.Reset()
    self.sync_sequence_number += 1
    wrapped_request_bytes = protocols.pack(protocols.RpcRequestWrapper,
        {   'requestor': self.response_queue,
            'method': method_descriptor.name,
            'payload': request.SerializeToString(),
            'sequence_number': self.sync_sequence_number
        }
        )
    #print ' '.join([hex(ord(c))[2:] for c in request.SerializeToString()])    
    #print ' '.join([hex(ord(c))[2:] for c in request_wrapper])
    
    try_again = True
    retries = 0
    while try_again:
        logger.debug("About to publish to exchange '%s' with key '%s'" % (self.exchange, self.request_routing_key))
        self.messagebus.basic_publish(amqp.Message(wrapped_request_bytes),
                        exchange=self.exchange,
                        routing_key=self.request_routing_key)
        
        # Wait for a response
        logger.debug("Waiting for a response on queue '%s'" % self.response_queue)
        response = None
        #retries = 0
        timeout_flag = False
        t0 = datetime.now()
        while not response:
            delta_t = datetime.now() - t0
            if delta_t.seconds * 1000 + delta_t.microseconds / 1000 > self.timeout_ms:
                timeout_flag = True
                break
            response = self.messagebus.basic_get(self.response_queue, no_ack=True) # returns a message or None
            time.sleep(0.01)
        
        if timeout_flag:
            logger.debug("RPC method '%s' timed out," % method_descriptor.name)
            if retries < self.max_retries:
                retries += 1
                logger.debug("Retrying (%d)." % retries)
                continue # out to try_again loop.  resets timer
            else:
                try_again = False
                rpc_controller.SetTimedOut()
                rpc_controller.SetFailed("Too many retries.")
                if done:
                    done(None)
                return None
        else:
            logger.info("Got a response in %s" % str(datetime.now() - t0))
            try_again = False
        
            response_wrapper = protocols.unpack(protocols.RpcResponseWrapper, response.body)
            if response_wrapper.sequence_number and response_wrapper.sequence_number != self.sync_sequence_number:
                errtext = "Response out of sequence. (Expected %d, got %d)  Purging the response queue." % (self.sync_sequence_number, response_wrapper.sequence_number)
                logger.error(errtext)
                self.messagebus.queue_purge(queue=self.response_queue) # clear the response queue and try again
                if retries < self.max_retries:
                    retries += 1
                    try_again = True
                    continue # out to try_again loop.  resets timer
                else:
                    rpc_controller.SetFailed("Sync problems. Too many retries.")
                    return None
            if response_wrapper.error:
                logger.error("RPC response error: %s" % response_wrapper.error_string)
                rpc_controller.SetFailed(response_wrapper.error)
                if done:
                    done(None)
                    return None
                
        response = protocols.unpack(response_class, response_wrapper.payload)
        logger.debug("Response is: %s" % str(response))
        if done:
            done(response)
        return response

class AmqpService(object):
    '''
    A wrapper class that takes care of the business of initialization:
        - Creates an RpcChannel with the given parameters
        - Instantiates a protobuf service of the given class
        - Delegates further attribute access to the Service instance.
        
    '''
    class ParameterMissing(Exception):
        pass
        
    def __init__(self, 
        #pb_service_class=None,
        amqp_channel=None,
        exchange='Control_Exchange',
        request_routing_key=None,
        reply_queue=None,
        timeout_ms=3000,
        max_retries=3):
        
        self.amqp_channel = amqp_channel or MessageBus().channel
        self.exchange = exchange
        
        # Required parameters:
        for param in ('reply_queue', 'request_routing_key'):
            if locals()[param]:
                setattr(self, param, locals()[param])
            else:
                raise AmqpService.ParameterMissing("%s is a required parameter." % param)
                
        self.rpc_channel = RpcChannel(self.exchange, self.reply_queue, 'dispatch', max_retries=3, timeout_ms=timeout_ms)
        #self.rpc_service = service_stub_class(self.rpc_channel)
        self.amqp_rpc_controller = AmqpRpcController()
        
    def _rpc_failure(self):
        '''Should be called when RPC calls fail (i.e. return value == None)'''
        if self.amqp_rpc_controller.TimedOut():
                self.logger.error("RPC request timed out.")
        elif self.amqp_rpc_controller.Failed():
                self.logger.error("Error in RPC: " + str(self.amqp_rpc_controller.ErrorText()))
                if 'Sync problems' in str(self.amqp_rpc_controller.ErrorText()):
                    raise Exception("Sync error caused RPC Failure!")
        else:
            assert False # If this happens, we're missing a failure state.
        self.amqp_rpc_controller.Reset()
   
    # Delegate other attribute access to protobuf rpc_service instance
    #def __getattr__(self, name):
    #    return object.__getattribute__(self.rpc_service, name)
        
class AmqpRpcEndpoint(object):

    class ParameterMissing(Exception):
        pass
        
    def __init__(self, 
            connection=None,
            exchange=None, 
            queue=None,
            default_timeout = 3000, # milliseconds
            ):

        for param in ('exchange', 'queue'):
            if not getparam(self, param, None):
                raise ParameterMissing("%s is a required parameter" % param)

        if connection:
            self.connection = connection
        else:
            self.connection = mb_connection # defined in the messaging.messagebus module
            
        self.amqp_channel =  self.connection.channel()
        
        self.amqp_channel.exchange_declare(exchange, 'direct')
        self.amqp_channel.queue_declare(queue, auto_delete=True)
        
        self.incoming_messages = Queue()
        self.consumption_thread = ConsumptionThread(connection=self.connection)
        self.consumption_thread.set_callback(queue=self.queue, callback=self.incoming_message_callback, no_ack=True)
        self.consumption_thread.start()
    
    def serialize_message(self, pb_message):
        ''' Serializes a protobuf message into an array of bytes, ready for transport '''
        bytes = pb_message.SerializeToString()
        return bytes
        
    def parse_message(self, messageclass, bytes):
        ''' Parse an array of bytes into a protobuf message. '''
        message = messageclass()
        message.ParseFromString(bytes)
        return message
        
    def send_message(self, message, routing_key):
        ''' Serialize and send a protobuf message along a specified AMQP route '''
        self.messagebus.publish(self.serialize_message(message), exchange=self.exchange, routing_key=routing_key)
        
    def get_message(self,messageclass, timeout=-2):
        '''
            Read and deserialize a protobuf message from the wire.
            For timeout, -2 means "use default", -1 means "none", and anything
            else means "timeout" milliseconds
        '''
        bytes = self.get_bytes(timeout=timeout)
        self.parse_message(messageclass, bytes)
        return message
        
    def send_bytes(self, bytes, routing_key):
        ''' Send an array of bytes along a specified AMQP route '''
        self.amqp_channel.basic_publish(bytes, exchange=self.exchange, routing_key=routing_key)
        
    def incoming_message_callback(self, msg):
        self.incoming_messages.put(msg)
    
    def get_bytes(self, timeout = -2):
        '''
        Get an array of bytes from the incoming queue.
        For timeout, -2 means "use default", -1 means "none", and anything
        else means "timeout" milliseconds
        '''
        if timeout == -1:
            return self.incoming_messages.get()
        if timeout == -2:
            timeout = self.default_timeout
        return self.incoming_messages.get(True, timeout / 1000)
        
    def bind_service(self, pb_service, routing_key):
        ''' Bind an rpc service to a specified routing key '''
        self.service = pb_service
        self.routing_key = routing_key
        self.amqp_channel.queue_bind(self.queue, self.exchange, routing_key=self.routing_key)
        
    def unbind_service(self):
        raise NotImplementedError
    
    def incoming_message_queue_size(self):
        ''' NOTE: qsize() is approximate. '''
        return self.incoming_messages.qsize()
        
    def Reset(self):
        raise NotImplementedError
        
# Attribute setters are not so useful in python.
#    def set_default_timeout(self, timeout = -1):
#        ''' Change the default global timeout. -1 means "no timeout" '''
#        self.default_timeout = timeout
        
    def Failed(self):
        raise NotImplementedError
        
class AmqpRpcClient(AmqpRpcEndpoint):
    def CallMethod( method_descriptor,
                    rpc_controller,
                    request,
                    response,
                    done):
        pass
                    
    
class AmqpRpcServer(AmqpRpcEndpoint):
    def __init__(
        amqp_channel = None
        
    ):
        self.terminate = False
        self.queries_processed = 0
        self.bytes_processed = 0
        
    def run(self):
        pass
    
    def shutdown(self):
        self.terminate = True
        
    def reset_stats(self):
        self.stats.reset()
        
    pass