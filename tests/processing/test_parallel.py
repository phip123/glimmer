import logging
import multiprocessing
import unittest

import pypeline.processing.factory as factory
from pypeline.processing import Source, Operator, Sink
from pypeline.util.context import Context

logging.basicConfig(level=logging.DEBUG)


class ParallelTopologyTest(unittest.TestCase):

    def test_single_source_and__sink_topology(self):
        class TestSource(Source):
            name = 'test-source'

            def read(self, out):
                out(1)

        class TestOp1(Operator):
            name = 'test-op-1'

            def apply(self, data, out):
                out(data + 1)

        class TestOp2(Operator):
            name = 'test-op-2'

            def apply(self, data, out):
                out(data - 1)

        class TestSink(Sink):
            name = 'test-sink'

            def write(self, data):
                value1 = data[TestOp1.name]
                value2 = data[TestOp2.name]
                q: multiprocessing.Queue = self.ctx.get('q')
                q.put(value1 + value2)

        source = TestSource()
        op1 = TestOp1()
        op2 = TestOp2()
        q = multiprocessing.Queue()
        sink_config = {
            'q': q
        }
        sink = TestSink(Context(config=sink_config))

        # Connect nodes
        source | [op1, op2]
        sink.receive_from([op1, op2])

        # Make Environment
        stop = multiprocessing.Event()
        env = factory.mk_parallel_env([source], stop=stop)

        # Start execution
        env.start(stop)

        value = q.get(timeout=2)
        self.assertEqual(value, 2)
        stop.set()

    def test_multiple_sources_single_sink(self):
        class TestSource1(Source):
            name = 'test-source-1'

            def read(self, out):
                out(1)

        class TestSource2(Source):
            name = 'test-source-2'

            def read(self, out):
                out(2)

        class TestOp1(Operator):
            name = 'test-op-1'

            def apply(self, data, out):
                out(data + 1)

        class TestOp2(Operator):
            name = 'test-op-2'

            def apply(self, data, out):
                out(data - 1)

        class TestSink(Sink):
            name = 'test-sink'

            def write(self, data):
                value1 = data[TestOp1.name]
                value2 = data[TestOp2.name]
                q: multiprocessing.Queue = self.ctx.get('q')
                q.put(value1 + value2)

        source1 = TestSource1()
        source2 = TestSource2()
        op1 = TestOp1()
        op2 = TestOp2()
        q = multiprocessing.Queue()
        sink_config = {
            'q': q
        }
        sink = TestSink(Context(config=sink_config))

        # Connect nodes
        source1 | op1
        source2 | op2
        sink.receive_from([op1, op2])

        # Make Environment
        stop = multiprocessing.Event()
        env = factory.mk_parallel_env([source1, source2], stop=stop)

        # Start execution
        env.start(stop)

        value = q.get(timeout=2)
        self.assertEqual(value, 3)
        stop.set()

    def test_single_source_multiple_sinks(self):
        class TestSource(Source):
            name = 'test-source'
            sent = False
            def read(self, out):
                if not self.sent:
                    out(1)
                    self.sent = True

        class TestOp1(Operator):
            name = 'test-op-1'

            def apply(self, data, out):
                out(data + 1)

        class TestOp2(Operator):
            name = 'test-op-2'

            def apply(self, data, out):
                out(data - 1)

        class TestSink1(Sink):
            name = 'test-sink-1'

            def write(self, data):
                value1 = data[TestOp1.name]
                value2 = data[TestOp2.name]
                self.logger.info('here1')
                self.logger.info(value1)
                self.logger.info(value2)
                q: multiprocessing.Queue = self.ctx.get('q')
                q.put(value1 + value2)

        class TestSink2(Sink):
            name = 'test-sink-2'

            def write(self, data):
                value1 = data[TestOp1.name]
                value2 = data[TestOp2.name]
                self.logger.info('here2')
                q: multiprocessing.Queue = self.ctx.get('q')
                q.put(-1 * (value1 - value2))

        source = TestSource()
        op1 = TestOp1()
        op2 = TestOp2()
        q1 = multiprocessing.Queue()
        q2 = multiprocessing.Queue()

        sink_config_1 = {
            'q': q1
        }

        sink_config_2 = {
            'q': q2
        }

        sink1 = TestSink1(Context(config=sink_config_1))
        sink2 = TestSink2(Context(config=sink_config_2))

        # Connect nodes
        source | [op1, op2]
        sink1.receive_from([op1, op2])
        sink2.receive_from([op1, op2])

        # Make Environment
        stop = multiprocessing.Event()
        env = factory.mk_parallel_env([source], stop=stop)

        # Start execution
        env.start(stop)

        value = q1.get(timeout=2)
        self.assertEqual(value, 2)

        value = q2.get(timeout=2)
        self.assertEqual(value, -2)

        stop.set()
