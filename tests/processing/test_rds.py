import unittest

from pypeline.processing.rds import RedisSource
from tests.testutils import RedisResource, assert_poll


class RedisSourceTest(unittest.TestCase):

    def setUp(self) -> None:
        self.redis_resource = RedisResource()
        self.redis_resource.setUp()
        self.redis = self.redis_resource.rds
        self.channel = 'mock:sensor:channel'
        self.sensor_id = "%s:%s" % (self.channel, "id")
        self.redis_source = RedisSource(self.redis, "%s:*" % self.channel)

    def tearDown(self) -> None:
        self.redis_resource.tearDown()

    def test_read(self):
        self.redis.publish(self.sensor_id, 'hello world')
        assert_poll(lambda: self.assert_read('hello world'))

    def test_multiple_reads(self):
        self.redis.publish(self.sensor_id, 'hello world')
        self.redis.publish(self.sensor_id, 'hello world2')
        self.redis.publish(self.sensor_id, 'hello world3')

        assert_poll(lambda: self.assert_read('hello world'))
        assert_poll(lambda: self.assert_read('hello world2'))
        assert_poll(lambda: self.assert_read('hello world3'))

    def assert_read(self, data: str):
        read = self.redis_source.read()
        return read and read.data == data
