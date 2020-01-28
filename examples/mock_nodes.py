import json
import logging
import time
from dataclasses import dataclass
from typing import Optional, List

from pypeline.processing import Source, Sink, Operator
from pypeline.util import EnhancedJSONEncoder

logger = logging.getLogger(__name__)


@dataclass
class DevMessage:
    id: str
    data: str


@dataclass
class DevReading:
    value: float
    unit: str
    time: float


@dataclass
class DevIdReading:
    id: str
    reading: DevReading


class DevAvgOperator(Operator[DevIdReading, DevIdReading]):
    name = 'dev_avg'
    last = None

    def apply(self, data: DevIdReading, out):
        if self.last:
            avg = (data.reading.value + self.last) / 2
        else:
            avg = data.reading.value
        self.last = avg
        out(DevIdReading(data.id, DevReading(avg, data.reading.unit, data.reading.time)))


class DevOperator(Operator[DevMessage, DevIdReading]):
    name = 'dev_op'

    def apply(self, msg: DevMessage, out):
        reading = json.loads(msg.data)
        out(DevIdReading(msg.id, DevReading(**reading)))


class DevSink(Sink):
    name = 'dev_sink'

    def open(self):
        super().open()
        logger.info(f'Sink connects to host: {self.ctx.getenv("host")}')

    def write(self, data):
        logger.info(f'write data: {data}')


class DevSource(Source[Optional[DevMessage]]):
    name = 'dev_src'
    index = 0

    def open(self):
        super().open()
        logger.info(f'Source connects to host: {self.ctx.getenv("host")}')

    readings: List[DevReading] = [
        DevReading(21, 'watt', time.time()),
        DevReading(5, 'watt', time.time()),
        DevReading(86, 'watt', time.time()),
        DevReading(100, 'watt', time.time()),
        DevReading(20, 'watt', time.time())
    ]

    data: List[DevMessage] = [
        DevMessage('id1', json.dumps(readings[0], cls=EnhancedJSONEncoder)),
        DevMessage('id2', json.dumps(readings[1], cls=EnhancedJSONEncoder)),
        DevMessage('id3', json.dumps(readings[2], cls=EnhancedJSONEncoder)),
        DevMessage('id4', json.dumps(readings[3], cls=EnhancedJSONEncoder)),
        DevMessage('id5', json.dumps(readings[4], cls=EnhancedJSONEncoder))
    ]

    def read(self, out):
        time.sleep(int(self.ctx.getenv('sleep')))
        msg = self.data[self.index % len(self.data)]
        self.index += 1
        out(msg)
