from amqplib import client_0_8 as amqp
from amq_config import connection_params
import threading
import logging
logger = logging.getLogger('messagebus')
DEFAULT_EXCHANGE = 'amq.direct'

connection = amqp.Connection(**connection_params)

class LazyProperty(object):
    ''' http://blog.pythonisito.com/2008/08/lazy-descriptors.html '''
    def __init__(self, func):
        self._func = func
        self.__name__ = func.__name__
        self.__doc__ = func.__doc__

    def __get__(self, obj, klass=None):
        if obj is None: return None
        result = obj.__dict__[self.__name__] = self._func(obj)
        return result

class ConsumptionThread(threading.Thread):
    def __init__(self, connection, shutdown_event):
        threading.Thread.__init__(self)
        self.daemon = True
        self.name = "consumption_thread"
        self.shutdown_event = shutdown_event
        #self.channel = connection.channel()
    
    @LazyProperty
    def channel(self):
        return connection.channel()
    
    def run(self):
        logger.info("Starting consume loop on channel %d" % self.channel.channel_id)
        while self.channel.callbacks and not self.shutdown_event.is_set():
            self.channel.wait()
        logger.info("Consume loop terminating, channel %d" % self.channel.channel_id)

class MessageBus(object):
    def __init__(self, **kwargs):
        self.shutdown_event = threading.Event()
        # Open the connection to RabitMQ
        #self._conn = amqp.Connection(**connection_params)
        if len(kwargs) == 0 or kwargs == connection_params:
            #use the default connection.
            self._conn = connection
        else:
            #merge kwargs and connection_params
            params = connection_params.copy()
            params.update(kwargs)
            self._conn = ampq.Connection(**params)
            
        # Properties below have beem made lazy
        #self._chan = self._conn.channel()
        #self.consumption_thread = ConsumptionThread(self._conn, self.shutdown_event)

    @LazyProperty
    def _chan(self):
        return self._conn.channel()
        
    @property
    def connection(self):
        return self._conn
    @property
    def channel(self):
        return self._chan
       
    def __del__(self):
        self.shutdown_event.set()
        try:
            self._chan.close()
        except TypeError:
            logger.error("Couldn't close the channel on MessageBus delete.  It may already have been closed...")

    @LazyProperty
    def consumption_thread(self):
        return ConsumptionThread(self._conn, self.shutdown_event)
    
    def publish(self, msg, exchange=DEFAULT_EXCHANGE, routing_key=None):
        msg = amqp.Message(msg)
        msg.properties["delivery_mode"] = 2   # Sets as persistent
        self._chan.basic_publish(msg,exchange=exchange,routing_key=routing_key)

    def setup_direct_queue(self, queue, exchange=DEFAULT_EXCHANGE, routing_key=None, chan=None):
        ''' Simplifies the job of creating a directly-routed queue by declaring the exchange, queue, and binding.
            If you don't specify a routing key, the queue name will be used.
        '''
        if not routing_key:
            routing_key = queue
        if not chan:
            chan = self._chan
        chan.exchange_declare(exchange=exchange, type="direct", durable=True, auto_delete=False,)
        chan.queue_declare(queue=queue, durable=True, exclusive=False, auto_delete=False)
        chan.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key)
        logger.debug("MessageBus (Ch#%d): Bound queue '%s' to exchange '%s' with routing key '%s'." % (chan.channel_id, queue, exchange, routing_key) )

    def basic_consume(self, *args, **kwargs):
        """Consumption duties are delegated to the consumer_thread's channel. """
        self.consumption_thread.channel.basic_consume(*args, **kwargs)
    
    def basic_ack(self, *args, **kwargs):
        """ ack duties are delegated to the consumer_thread's channel. """
        self.consumption_thread.channel.basic_ack(*args, **kwargs)
    
    def register_consumer(self, queuename, callback, exchange=DEFAULT_EXCHANGE, routing_key=None, chan=None):
        '''
        Declare a direct queue and attach a consumer to it.  This assumes an exchange has already beed created.
        If you don't specify a routing key, the queue name will be used.
        Returns the consumer tag.
        '''
        if not routing_key:
            routing_key = queuename
        if not chan:
            #chan = self._chan
            chan = self.consumption_thread.channel
        chan.queue_declare(queue=queuename, durable=True, auto_delete=False)
        chan.queue_bind(queue=queuename, exchange=exchange, routing_key=routing_key)
        logger.debug("MessageBus (Ch#%d): Bound queue '%s' to exchange '%s' with routing key '%s'." % (chan.channel_id, queuename, exchange, routing_key) )
        logger.debug("MessageBus (Ch#%d): %s will consume from queue '%s'" % (chan.channel_id, str(callback), queuename))
        return chan.basic_consume(callback=callback, queue=queuename)

        
    def start_consuming(self):
        self.consumption_thread.start()
        


    def __getattr__(self, name):
        return object.__getattribute__(self._chan, name)
 