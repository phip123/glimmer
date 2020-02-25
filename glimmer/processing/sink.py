from glimmer.processing import Sink
from glimmer.util.context import Context


class NoopSink(Sink):
    name = 'noop'

    def __init__(self, ctx: Context = None) -> None:
        super().__init__(ctx)
        self.logger = ctx.create_logger(__name__)

    def write(self, data):
        self.logger.info("noop received: %s" % data)
