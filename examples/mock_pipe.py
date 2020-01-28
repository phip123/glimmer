import logging
import sys
import threading
import os

from pypeline.processing.operator import LogOperator
from pypeline.processing.sync import SynchronousEnvironment, SynchronousTopology
from examples.mock_nodes import DevSource, DevSink, DevOperator, DevAvgOperator

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


class MockApp:

    def run(self):
        source = DevSource()
        sink = DevSink()
        op1 = DevOperator()
        op2 = DevAvgOperator()
        op3 = LogOperator()
        os.environ['home_controller_sleep'] = '2'
        os.environ['home_controller_host'] = 'localhost'
        composed = op1 - op2 - op3
        topology = SynchronousTopology(source, composed, sink)

        stop = threading.Event()
        env = None
        try:
            env = SynchronousEnvironment(topology, stop)
            env.execute()
        except KeyboardInterrupt:
            stop.set()
            env.close()


if __name__ == "__main__":
    app = MockApp()
    app.run()
