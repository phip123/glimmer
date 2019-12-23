import logging
import threading
from typing import List, Optional

import pypeline.processing.registry as registry
from pypeline.processing import Sink, Source, Operator, compose_list
from pypeline.processing.sync import SequentialTopology, SequentialPipe

logger = logging.getLogger(__name__)


class ControllerDaemon:

    def __init__(self, operator_names: List[str], source_name: str, sink_name: str, stop: threading.Event = None):
        self.stop = stop or threading.Event()
        self.operator_names = operator_names
        self.source_name = source_name
        self.sink_name = sink_name
        self.sink = None
        self.source = None
        self.operators = []
        self.topology = None
        self.env = None

    def run(self):
        self.sink = self._get_sink()
        self.source = self._get_source()
        self.operators = self._get_operators()

        if self.sink and self.source:
            composed = None
            if len(self.operators) > 0:
                composed = compose_list(self.operators)

            self.topology = SequentialTopology(self.source, composed, self.sink)
            self.env = SequentialPipe(self.topology, self.stop)
            self.env.execute()
        else:
            logger.error("No sink and/or source specified")
            return

    def close(self):
        if self.sink:
            self.sink.close()

        if self.source:
            self.source.close()

    def _get_sink(self) -> Optional[Sink]:
        return registry.get_sink(self.sink_name)

    def _get_source(self) -> Optional[Source]:
        return registry.get_source(self.source_name)

    def _get_operators(self) -> List[Operator]:
        return [registry.get_operator(x) for x in self.operator_names if registry.get_operator(x)]
