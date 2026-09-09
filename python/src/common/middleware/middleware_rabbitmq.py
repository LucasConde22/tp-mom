import pika
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.queue_name = queue_name
        self.channel.queue_declare(queue_name)
    
    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            on_message_callback(body,
                                lambda: ch.basic_ack(method.delivery_tag),
                                lambda: ch.basic_nack(method.delivery_tag))

        self.channel.basic_consume(queue=self.queue_name,
                                   auto_ack=False,
                                   on_message_callback=callback)
        self.channel.start_consuming()

    def stop_consuming(self):
        self.channel.stop_consuming()

    def send(self, message):
        self.channel.basic_publish(exchange='',
                                   routing_key=self.queue_name,
                                   body=message)

    def close(self):
        self.connection.close()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    DIRECT_EXCHANGE_TYPE = 'direct'
    
    def __init__(self, host, exchange_name, routing_keys):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
                    exchange=exchange_name,
                    exchange_type=__class__.DIRECT_EXCHANGE_TYPE
        )

        self.queue_name = self.channel\
                                    .queue_declare('', exclusive=True)\
                                    .method.queue

        for routing_key in routing_keys:
            self.channel.queue_bind(exchange=exchange_name,
                                    queue=self.queue_name,
                                    routing_key=routing_key)

    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            on_message_callback(body,
                                lambda: ch.basic_ack(method.delivery_tag),
                                lambda: ch.basic_nack(method.delivery_tag))

        self.channel.basic_consume(queue=self.queue_name,
                                    auto_ack=False,
                                    on_message_callback=callback)
        self.channel.start_consuming()

    def stop_consuming(self):
        self.channel.stop_consuming()

    def send(self, message):
        for key in self.routing_keys:
            self.channel.basic_publish(exchange=self.exchange_name,
                                       routing_key=key,
                                       body=message)

    def close(self):
        self.connection.close()
