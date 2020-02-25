import logging
import multiprocessing
import os
import sys

from examples.mock_nodes import DevSource, DevSink, DevOperator, DevAvgOperator
from glimmer.processing.operator import LogOperator
from glimmer.processing.sync import SynchronousEnvironment, SynchronousTopology

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

        env = None
        try:
            env = SynchronousEnvironment(topology)
            env.run()
        except KeyboardInterrupt:
            env.stop()
            env.close()


if __name__ == "__main__":
    app = MockApp()
    app.run()
