from amqplib import client_0_8 as amqp
from amq_config import connection_params

import logging
logger = logging.getLogger()
DEFAULT_EXCHANGE = 'ngt.direct'

connection = amqp.Connection(**connection_params)

class MessageBus(object):
    def __init__(self, **kwargs):
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
        self._chan = self._conn.channel()

    def __del__(self):
        self._chan.close()
        #self._conn.close()

    @property
    def connection(self):
        return self._conn
    @property
    def channel(self):
        return self._chan

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

    def register_consumer(self, queuename, callback, exchange=DEFAULT_EXCHANGE, routing_key=None, chan=None):
        '''
        Declare a direct queue and attach a consumer to it.  This assumes an exchange has already beed created.
        If you don't specify a routing key, the queue name will be used.
        Returns the consumer tag.
        '''
        if not routing_key:
            routing_key = queuename
        if not chan:
            chan = self._chan
        chan.queue_declare(queue=queuename, durable=True, auto_delete=False)
        chan.queue_bind(queue=queuename, exchange=exchange, routing_key=routing_key)
        logger.debug("MessageBus (Ch#%d): Bound queue '%s' to exchange '%s' with routing key '%s'." % (chan.channel_id, queuename, exchange, routing_key) )
        logger.debug("MessageBus (Ch#%d): %s will consume from queue '%s'" % (chan.channel_id, str(callback), queuename))
        return chan.basic_consume(callback=callback, queue=queuename)


    def ack(self, *args, **kwargs):
        self._chan.basic_ack(*args, **kwargs)

    def __getattr__(self, name):
        return object.__getattribute__(self._chan, name)
 