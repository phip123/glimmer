import unittest
from unittest.mock import patch

from pypeline.processing.rabbitmq import RabbitMqSink
from tests.testutils import TestRabbitMqChannel


class RabbitMqSinkTest(unittest.TestCase):

    def setUp(self) -> None:
        self.exchange = 'exchange'
        self.routing_key = 'routing_key'
        self.sink = RabbitMqSink(TestRabbitMqChannel(), self.exchange, self.routing_key)

    @patch('tests.testutils.TestRabbitMqChannel.publish')
    def test_write(self, publish):
        self.sink.write('data')
        publish.assert_called_once_with(exchange=self.exchange, routing_key=self.routing_key, data='data')
