import abc

import pika

from pypeline.processing import Sink


class RabbitMqChannel(abc.ABC):

    def publish(self, exchange: str, routing_key: str, data: str):
        raise NotImplementedError

    def close(self):
        pass


class PikaRabbitMqChannel(RabbitMqChannel):

    def __init__(self, connection: pika.BlockingConnection):
        self.conn = connection
        self.channel = connection.channel()

    def publish(self, exchange: str, routing_key: str, data: str):
        self.channel.basic_publish(exchange=exchange, routing_key=routing_key, body=data)

    def close(self):
        self.conn.close()


class RabbitMqSink(Sink[str]):
    name = 'rabbitmq'

    def __init__(self, channel: RabbitMqChannel = None, exchange: str = None, routing_key: str = None,
                 ctx=None):
        super().__init__(ctx)
        self.exchange = exchange
        self.routing_key = routing_key
        self.channel = channel

    def open(self):
        super().open()
        if not self.channel:
            self.exchange = self.ctx.get_rabbitmq_exchange()
            self.routing_key = self.ctx.get_rabbitmq_routing_key()
            self.channel = PikaRabbitMqChannel(self.ctx.create_rabbit_connection())

    def write(self, data: str):
        self.logger.debug(
            'publish in exchange %s under routing_key %s data: %s' % (self.exchange, self.routing_key, data))
        self.channel.publish(exchange=self.exchange, routing_key=self.routing_key, data=data)

    def close(self):
        super().close()
        if self.channel:
            self.channel.close()
