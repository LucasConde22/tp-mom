from multiprocessing.dummy import connection

import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.queue_name = queue_name
        self.channel.queue_declare(queue_name)
    
    def start_consuming(self, on_message_callback):
        pass

    def stop_consuming(self):
        pass

    def send(self, message):
        self.channel.basic_publish(exchange='',
                                   routing_key=self.queue_name,
                                   body=message)

    def close(self):
        self.connection.close()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.routing_keys = routing_keys
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=exchange_name,
        )

    def start_consuming(self, on_message_callback):
        pass

    def stop_consuming(self):
        pass

    def send(self, message):
        pass

    def close(self):
        self.connection.close()
