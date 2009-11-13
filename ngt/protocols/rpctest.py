import threading, time, sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import protocols
from protocols.rpcServices import WireMessage
from pprint import pprint
from messaging.messagebus import MessageBus, amqp

def hexdump(str):
    return ' '.join([hex(ord(c))[2:] for c in str ])

class Bouncer(threading.Thread):
    daemon = True
    queuename = 'bouncer'
    response_class = protocols.EchoMessage
    request_class = protocols.EchoMessage

    def handlemsg(self, msg):
       print "In the message handler"
       wiremessage = WireMessage(msg.body)
       print "bouncer wire msg: %s" % hexdump(wiremessage.serialized_bytes)
       pprint(protocols.unpack(protocols.RpcRequestWrapper, wiremessage.serialized_bytes))
       wrapped_request = wiremessage.parse_as_message(protocols.RpcRequestWrapper)
       request_bytes = wrapped_request.payload
       request = protocols.unpack(self.request_class, request_bytes)
       pprint(request)
       response_bytes = protocols.pack(self.response_class, {'payload': ''.join(reversed(request.payload))})
       responsewire = WireMessage.response({'payload':response_bytes, 'error':False})
       self.mb.basic_publish(amqp.Message(responsewire.serialized_bytes), exchange='amq.direct', routing_key=wrapped_request.requestor)
       print "Bouncer published a result with key '%s'" % wrapped_request.requestor


    def run(self):
        self.mb = MessageBus()
        self.mb.queue_delete(self.queuename )
        self.mb.queue_declare(self.queuename )
        self.mb.queue_bind(self.queuename, 'amq.direct', routing_key=self.queuename)
        print "Bouncer Go! Consuming from %s" % self.queuename      
        while True:
            sys.stdout.write('.'); sys.stdout.flush()
            msg = self.mb.basic_get(queue=self.queuename, no_ack=True)
            if msg:
                print "Bouncer got a message."
                self.handlemsg(msg)
                break
            time.sleep(0.1)    


def test():
    bouncer = Bouncer()
    bouncer.start()
    channel = protocols.rpcServices.RpcChannel('amq.direct', 'test', Bouncer.queuename)
    service = protocols.ReaperCommandService_Stub(channel)
    controller = protocols.rpcServices.RpcController()
    request = protocols.EchoMessage()
    request.payload = 'Howdy!'
    response = service.Echo(controller, request, None)
    print "Got a response: ", response

if __name__ == '__main__':
    test()
