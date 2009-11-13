import threading, time, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import protocols
from protocols.rpcServices import WireMessage
from pprint import pprint
from messaging.messagebus import MessageBus, amqp

"""
    This is a test and reference client for RPC services over AMQP via protocol buffers.
    The service implementation can be found in rpcServices.py
"""

def hexdump(str):
    return ' '.join([hex(ord(c))[2:] for c in str ])

class Bouncer(threading.Thread):
    daemon = True
    queuename = 'bouncer'
    
    
    def bounce(self, requestbytes):
        """ Takes a reqest_class message as bytes and returns a response_class message as bytes.
            Only the method being called needs to care about the message types it accepts and returns. (both EchoMessage in this case)
        """
        response_msg_class = protocols.EchoMessage
        request_msg_class = protocols.EchoMessage
        
        request = protocols.unpack(request_msg_class, requestbytes)
        print "Bouncing! ",
        pprint(request)
        if 'hate you' in request.echotext:
            print "Failing due to hate crimes."
            raise Exception("I hate you too.")
        elif request.echotext == 'force timeout':
            time.sleep(600)
        response = protocols.dotdict()
        response.echotext = ''.join(reversed(request.echotext))
        return protocols.pack(response_msg_class,response)

    def handlemsg(self, msg):
       """ Accepts an AMQP Message with a WireMessage payload.
           Decodes the wiremessage and dispatches its RpcRequestWrapper payload (raw bytes) to the appropriate handler command.
           Takes the response, wraps it in an RpcResponseWrapper Message, wraps THAT in a WireMessage, and sends it back to the requestor.
       """
       wiremessage = WireMessage(msg.body)
       pprint(protocols.unpack(protocols.RpcRequestWrapper, wiremessage.serialized_bytes))
       wrapped_request = wiremessage.parse_as_message(protocols.RpcRequestWrapper)
       request_bytes = wrapped_request.payload
       
       assert wrapped_request.method == 'Echo'  # or multiplex here to dispatch to different methods
       
       try:
           response_bytes = self.bounce(request_bytes)
           wireresponse = WireMessage.response({'payload':response_bytes, 'error':False})
       except Exception, e:
           wireresponse = WireMessage.response({'payload':'', 'error':True, 'error_string': str(e)})
       
       self.mb.basic_publish(amqp.Message(wireresponse.serialized_bytes), exchange='amq.direct', routing_key=wrapped_request.requestor)
       print "Bouncer published a result with key '%s'" % wrapped_request.requestor


    def run(self):
        self.mb = MessageBus()
        self.mb.queue_delete(self.queuename ) # clear the queueu
        self.mb.queue_declare(self.queuename )
        self.mb.queue_bind(self.queuename, 'amq.direct', routing_key=self.queuename)
        print "Bouncer Go! Consuming from queue '%s'" % self.queuename      
        while True:
            #sys.stdout.write('.'); sys.stdout.flush()
            msg = self.mb.basic_get(queue=self.queuename, no_ack=True)
            if msg:
                print "Bouncer got a message."
                self.handlemsg(msg)
            time.sleep(0.1)

flag = threading.Event()
def test():
    bouncer = Bouncer()
    bouncer.start()
    channel = protocols.rpcServices.RpcChannel('amq.direct', 'test', Bouncer.queuename)
    service = protocols.ReaperCommandService_Stub(channel)
    controller = protocols.rpcServices.AmqpRpcController()
    request = protocols.EchoMessage()
       
    time.sleep(0.2)
                    
    # Test the success case
    print "\nTesting for success..."
    request.echotext = 'Howdy!'
    response = service.Echo(controller, request, None)
    print "Got a response: ", response
    if response.echotext == '!ydwoH':
        print "Success!"
    
    #Test the failure case
    print "\nTesting for failure..."
    request.echotext = "I hate you."
    response = service.Echo(controller, request, None)
    assert response == None
    print "Failure succeded."
    
    #Test Callbacks
    print "\nTesting callbacks."
    flag.clear()
    def callmeback(msg):
        flag.set()
        print "In callback: ", str(msg)
    request.echotext = "ping"
    service.Echo(controller, request, callmeback)
    assert flag.is_set()
    
    # Test Timeout
    print "\nTesting Timeout"
    request.echotext = "force timeout"
    response = service.Echo(controller, request, None)
    assert response == None
    print "If this took less than 10 minutes, the Request timed out. (That's good)."


if __name__ == '__main__':
    test()
