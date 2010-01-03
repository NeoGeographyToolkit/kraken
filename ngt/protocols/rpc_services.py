import sys, os, time
from datetime import datetime
import google.protobuf
from google.protobuf.service import RpcController as _RpcController, Service
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))
from ngt import protocols
from ngt.messaging.messagebus import MessageBus, amqp
import logging
logging.basicConfig()
logger = logging.getLogger('amqprpc')
logger.setLevel(logging.INFO)
logger.debug("Testing logger.")

class SanityError(Exception):
    ''' Raise this when things that should never happen happen. '''
    pass

class AmqpRpcController(_RpcController):

  """An RpcController mediates a single method call.

  This RpcController implementation can mediate method calls via amqp.
  """
  
  def __init__(self, timeout_ms=5000):
      self.m_failed = False
      self.m_failed_reason = ''
      self.m_timeout_flag = False
      self.m_timeout_millis = timeout_ms # 5 Seconds

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
  def __init__(self, exchange, response_queue, request_routing_key):
      self.exchange = exchange
      self.response_queue = response_queue
      self.request_routing_key = request_routing_key
      self.messagebus = MessageBus()
      
      #TODO: Setup exchange & queue
      self.messagebus.exchange_declare(exchange, 'direct')
      #self.messagebus.queue_delete(queue=response_queue) # clear it in case there are backed up messages (EDIT: it *should* autodelete)
      self.messagebus.queue_declare(queue=response_queue, auto_delete=True)
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
    wrapped_request_bytes = protocols.pack(protocols.RpcRequestWrapper,
        {   'requestor': self.response_queue,
            'method': method_descriptor.name,
            'payload': request.SerializeToString()
        }
        )
    #print ' '.join([hex(ord(c))[2:] for c in request.SerializeToString()])    
    #print ' '.join([hex(ord(c))[2:] for c in request_wrapper])
    
    logger.debug("About to publish to exchange '%s' with key '%s'" % (self.exchange, self.request_routing_key))
    self.messagebus.basic_publish(amqp.Message(wrapped_request_bytes),
                    exchange=self.exchange,
                    routing_key=self.request_routing_key)
    
    # Wait for a response
    logger.debug("Waiting for a response on queue '%s'" % self.response_queue)
    response = None
    timeout_flag = False
    t0 = datetime.now()
    while not response:
        delta_t = datetime.now() - t0
        if delta_t.seconds * 1000 + delta_t.microseconds / 1000 > rpc_controller.m_timeout_millis:
            timeout_flag = True
            break
        response = self.messagebus.basic_get(self.response_queue, no_ack=True) # returns a message or None
        time.sleep(0.01)
    
    if timeout_flag:
        logger.debug("RPC method '%s' timed out," % method_descriptor.name)
        rpc_controller.SetTimedOut()
        if done:
            done(None)
        return None
    else:
        logger.debug("Got some sort of response")
    
        response_wrapper = protocols.unpack(protocols.RpcResponseWrapper, response.body)
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
