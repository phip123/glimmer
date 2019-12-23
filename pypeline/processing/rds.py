import time
from dataclasses import dataclass
from typing import Optional, Tuple

import redis

from pypeline.processing import Source, Sink
from pypeline.util.context import Context


@dataclass
class RedisMessage:
    channel: str
    data: str


class RedisSource(Source[Optional[RedisMessage]]):
    name = 'rds'

    def __init__(self, rds: redis.Redis = None, pattern: str = None, ctx: Context = None):
        super().__init__(ctx)
        self.rds = rds
        self.pattern = pattern
        self.p = None
        if self.rds:
            self._init_pubsub()

    def open(self):
        super().open()
        if not self.rds:
            self.rds = self.ctx.create_redis()
            self.pattern = "%s:*" % self.ctx.get_redis_base_key()
            self._init_pubsub()

    def close(self):
        super().close()

    def read(self) -> Optional[RedisMessage]:
        msg = self.p.get_message()
        if msg and msg['type'] == 'pmessage':
            return RedisMessage(msg['channel'], msg['data'])
        return None

    def _init_pubsub(self):
        self.p = self.rds.pubsub()
        self.p.psubscribe(self.pattern)
        self.read()  # read because first message is subscription


class SleepingRedisSource(RedisSource):
    name = 'srds'

    def __init__(self, rds: redis.Redis = None, pattern: str = None, ctx: Context = None):
        super().__init__(rds, pattern, ctx)
        self.sleep = ctx.getenv('sleep', 2)

    def read(self) -> Optional[RedisMessage]:
        read = super().read()
        time.sleep(self.sleep)
        return read


class RedisPublisherSink(Sink[Tuple[str, str]]):
    name = 'rds-pub'

    def __init__(self, rds: redis.Redis = None, channel: str = None, ctx: Context = None):
        super().__init__(ctx)
        self.rds = rds
        self.channel = channel
        self.p = None

    def open(self):
        super().open()
        if not self.rds:
            self.rds = self.ctx.create_redis()

    def write(self, data: Tuple[str, str]):
        self.logger.debug("publish under channel %s: %s" % (data[0], data[1]))
        self.rds.publish(data[0], data[1])
