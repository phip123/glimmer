import os
import shutil
import tempfile

import redislite

from pypeline.processing.rabbitmq import RabbitMqChannel
from pypeline.util import poll


class TestResource(object):

    def setUp(self):
        pass

    def tearDown(self):
        pass


class RedisResource(TestResource):
    tmpfile: str
    rds: redislite.Redis

    def setUp(self):
        self.tmpfile = tempfile.mktemp('.db', 'home_controller_test_')
        self.rds = redislite.Redis(self.tmpfile, decode_responses=True)
        self.rds.get('dummykey')  # run a first command to initiate

    def tearDown(self):
        self.rds.shutdown()

        os.remove(self.tmpfile)
        os.remove(self.rds.redis_configuration_filename)
        os.remove(self.rds.settingregistryfile)
        shutil.rmtree(self.rds.redis_dir)

        self.rds = None
        self.tmpfile = None


class TestRabbitMqChannel(RabbitMqChannel):

    def publish(self, exchange: str, routing_key: str, data: str):
        pass


def assert_poll(condition, msg='Condition failed'):
    try:
        poll(condition, 2, 0.01)
    except TimeoutError:
        raise AssertionError(msg)
