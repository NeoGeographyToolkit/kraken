from amqplib import client_0_8 as amqp

conn = amqp.Connection(host="localhost:5672",
                       userid="guest",
                       password="guest",
                       virtual_host="/",
                       insist=False)
chan = conn.channel()

chan.queue_declare(queue="remote_job", durable=True, exclusive=False, auto_delete=False)
chan.exchange_declare(exchange="ngt", type="direct", durable=True, auto_delete=False,)

chan.queue_bind(queue="remote_job", exchange="ngt", routing_key="jobs")

def recv_callback(msg):
    print 'Received: ' + msg.body + ' from channel #' + str(msg.channel.channel_id)

chan.basic_consume(queue='remote_job', no_ack=True, callback=recv_callback, consumer_tag="testtag")
while True:
    chan.wait()
chan.basic_cancel("testtag")


chan.close()
conn.close()
