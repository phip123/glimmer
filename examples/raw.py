import logging
import os
import threading

from examples.mock_nodes import DevAvgOperator, DevOperator, DevSource, DevSink
from pypeline.daemon import ControllerDaemon
from pypeline.processing import registry
from pypeline.processing.operator import LogOperator
from pypeline.util.context import Context

logger = logging.getLogger(__name__)


def main_raw_custom_registry():
    logging.basicConfig(level=logging._nameToLevel['DEBUG'])

    logger.info('Starting raw mock_nodes app')

    # Register all custom nodes
    registry.register_operator(DevAvgOperator(Context(config_name=DevAvgOperator.name)))
    registry.register_operator(DevOperator(Context(config_name=DevOperator.name)))
    registry.register_source(DevSource(Context(config_name=DevSource.name)))
    registry.register_sink(DevSink(Context(config_name=DevSink.name)))
    registry.register_operator(LogOperator())

    os.environ['home_controller_sleep'] = '2'
    os.environ['home_controller_host'] = 'localhost'

    # registry only knows nodes that are registered above
    daemon = None
    stop = threading.Event()
    try:
        op_names = [DevOperator.name, LogOperator.name, DevAvgOperator.name]
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
    main_raw_custom_registry()
