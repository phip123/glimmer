import logging
import os
import threading

from pypeline.daemon import ControllerDaemon
from pypeline.processing import registry
from pypeline.util.context import Context
from examples.mock_nodes import DevAvgOperator, DevOperator, DevSource, DevSink

logger = logging.getLogger(__name__)


def main_raw_with_defaults():
    logging.basicConfig(level=logging._nameToLevel['DEBUG'])

    logger.info('running mock_nodes app as cmd programm')

    # Register all custom nodes
    registry.register_operator(DevAvgOperator(Context(config_name=DevAvgOperator.name)))
    registry.register_operator(DevOperator(Context(config_name=DevOperator.name)))
    registry.register_source(DevSource(Context(config_name=DevSource.name)))
    registry.register_sink(DevSink(Context(config_name=DevSink.name)))

    os.environ['home_controller_sleep'] = '2'
    os.environ['home_controller_host'] = 'localhost'

    # registry  knows custom nodes and all default nodes
    daemon = None
    stop = threading.Event()
    try:
        op_names = [DevOperator.name, DevAvgOperator.name]
        source_name = DevSource.name
        sink_name = DevSink.name
        daemon = ControllerDaemon(op_names, source_name, sink_name, stop)
        daemon.run()
    except KeyboardInterrupt:
        stop.set()
    finally:
        logger.info('exiting controller dameon...')
        if daemon:
            daemon.close()


if __name__ == '__main__':
    main_raw_with_defaults()
