import logging

import pypeline.cli.controllerd  as controllerd
from pypeline.processing import registry
from pypeline.util.context import Context
from examples.mock_nodes import DevAvgOperator, DevOperator, DevSource, DevSink

logger = logging.getLogger(__name__)


def main_cli():
    logger.info('Starting cli mock_nodes app')

    # Register all custom nodes
    registry.register_operator(DevAvgOperator(Context(config_name=DevAvgOperator.name)))
    registry.register_operator(DevOperator(Context(config_name=DevOperator.name)))
    registry.register_source(DevSource(Context(config_name=DevSource.name)))
    registry.register_sink(DevSink(Context(config_name=DevSink.name)))

    # start cli, will create topology from program arguments
    controllerd.main()


if __name__ == '__main__':
    main_cli()
