import argparse
import logging
import os
import signal
import threading

from pypeline.daemon import ControllerDaemon

logger = logging.getLogger(__name__)


def run():
    main()


def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    parser = argparse.ArgumentParser()
    parser.add_argument('--logging', required=False,
                        help='set log level (DEBUG|INFO|WARN|...) to activate logging',
                        default=os.getenv('home_controller_logging_level'))

    parser.add_argument('--sink', required=True, help='Sink of the pipeline')
    parser.add_argument('--source', required=True, help='Source of the pipeline')
    parser.add_argument('--operators', nargs='*', help='A comma separated list of operators in the pipeline')

    args = parser.parse_args()

    if args.logging:
        logging.basicConfig(level=logging._nameToLevel[args.logging])

    sink_name = args.sink
    source_name = args.source
    operator_names = args.operators or []

    logger.info('starting controller daemon...')
    stop = threading.Event()
    daemon = None
    try:
        daemon = ControllerDaemon(operator_names, source_name, sink_name, stop)
        daemon.run()
    except KeyboardInterrupt:
        pass
    finally:
        logger.info('exiting controller daemon...')
        if daemon:
            daemon.close()


def signal_handler(signum, frame):
    raise KeyboardInterrupt


if __name__ == '__main__':
    main()
