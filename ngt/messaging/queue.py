from amqplib import client_0_8 as amqp
from amq_config import connection_params

# Manages a singleton instance of the connection to the RabbitMQ
# message server.  The actual implementation is contained in the
    # __impl class contained within MessageBus
class MessageBus(object):

    # These are hard coded for now, but should obviously be factored
    # out into some sort of settings class.
    #_HOST = "localhost:5672"
    #_USERID = "guest"
    #_PASSWORD = "guest"
    #_VIRTUAL_HOST = "/"


    # ---------------------------------------------------------------------
    #                       MessageBus Implementation
    # ---------------------------------------------------------------------

    class __impl:
        def __init__(self):
            # Open the connection to RabitMQ
            self._conn = amqp.Connection(**connection_params)
            self._chan = self._conn.channel()

        def __del__(self):
            self._chan.close()
            self._conn.close()

        def basic_publish(self, msg, exchange = "ngt", routing_key="jobs"):
            msg = amqp.Message(msg)
            msg.properties["delivery_mode"] = 2   # Sets as persistent
            return self._chan.basic_publish(msg,exchange=exchange,routing_key=routing_key)
            
        def setup_direct_queue(self, queuename, exchangename='ngt.direct', routing_key=None):
            '''Simplifies the job of creating a directly-routed queue.'''
            if not routing_key:
                routing_key = queuename
            chan = self._chan
            chan.queue_declare(queue=queuename, durable=True, exclusive=False, auto_delete=False)
            chan.exchange_declare(exchange=exchangename, type="direct", durable=True, auto_delete=False,)
            chan.queue_bind(queue=queuename, exchange=exchangename, routing_key=routing_key)

        def register_callback(self, recv_callback, queue = "ngt",
                              exchange = "ngt", routing_key="jobs"):

            # First we need to establish the endpoint so that messages
            # can be received here.
            chan.queue_declare(queue=queue, durable=True,
                               exclusive=False, auto_delete=False)
            chan.exchange_declare(exchange=exchange, type="direct",
                                  durable=True, auto_delete=False,)
            chan.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key)

            # Then we register the recv_callback with amqplib
            # self._chan.basic_consume(queue=queue, no_ack=True,
            #                          callback=recv_callback, consumer_tag="testtag")
            #while True:
            #    chan.wait()
            #chan.basic_cancel("testtag")
            

        def wait(self):
            pass
        #        self._chan.basic_consume(queue='remote_job', no_ack=True,
        #                           callback=recv_callback, consumer_tag="testtag")
        #        while True:
        #            chan.wait()
        #        chan.basic_cancel("testtag")


        #    def recv_callback(msg):
        #        print 'Received: ' + msg.body + ' from channel #' + str(msg.channel.channel_id)

    __instance = None

    # ---------------------------------------------------------------------

    def __init__(self):
        """ Create a singleton instance to the RabbitMQ message server """

        # Check to see if we already have an instance
        if (MessageBus.__instance is None):
            # Create and remember instance
            MessageBus.__instance = MessageBus.__impl();
            
        # Store instance reference as the only member in the handle
        self.__dict__['_MessageBus__instance'] = MessageBus.__instance


    def __getattr__(self, attr):
        """ Delegate access to implementation """
        return getattr(self.__instance, attr)

    def __setattr__(self, attr, value):
        """ Delegate access to implementation """
        return setattr(self.__instance, attr, value)



