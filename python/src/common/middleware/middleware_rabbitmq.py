import pika
import pika.exceptions
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError

# Errores que indican desconexión:
DISCONNECTED_ERRORS = (
    pika.exceptions.AMQPConnectionError,
    pika.exceptions.ConnectionClosed,
    pika.exceptions.StreamLostError,
    pika.exceptions.ChannelWrongStateError,
    pika.exceptions.ConnectionWrongStateError,
    pika.exceptions.IncompatibleProtocolError,
)

MSG_ERROR_CLOSED_CONNECTION = 'The connection has already been closed'

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.is_consuming = False
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
        self.is_consuming = True
        self.channel.start_consuming()

    def stop_consuming(self):
        if not self.is_consuming:
            return
        
        if not self.connection or self.connection.is_closed:
            raise MessageMiddlewareDisconnectedError(MSG_ERROR_CLOSED_CONNECTION)

        try:
            self.channel.stop_consuming()
        except DISCONNECTED_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(e)
        finally:
            self.is_consuming = False

    def send(self, message):
        self.channel.basic_publish(exchange='',
                                   routing_key=self.queue_name,
                                   body=message)

    def close(self):
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(e)

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    DIRECT_EXCHANGE_TYPE = 'direct'
    
    def __init__(self, host, exchange_name, routing_keys):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.is_consuming = False
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
        self.is_consuming = True
        self.channel.start_consuming()

    def stop_consuming(self):
        if not self.is_consuming:
            return
        
        if not self.connection or self.connection.is_closed:
            raise MessageMiddlewareDisconnectedError(MSG_ERROR_CLOSED_CONNECTION)

        try:
            self.channel.stop_consuming()
        except DISCONNECTED_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(e)
        finally:
            self.is_consuming = False

    def send(self, message):
        for key in self.routing_keys:
            self.channel.basic_publish(exchange=self.exchange_name,
                                       routing_key=key,
                                       body=message)

    def close(self):
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(e)
