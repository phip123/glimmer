import logging
import threading
from dataclasses import dataclass
from typing import TypeVar, Generic

from pypeline.processing import Source, Operator, Sink, Environment, Topology

Result = TypeVar("Result")
Out = TypeVar("Out")


@dataclass
class SequentialTopology(Topology, Generic[Result, Out]):
    """
    source: the source the env will read from
    operator: the operator the pipe will use for transforming the data
    """
    source: Source[Result]
    operator: Operator[Result, Out]
    sink: Sink[Out]


class SequentialPipe(Environment, Generic[Result, Out]):
    """
    This pipe will call each operator one after another without any kind of parallelism.
    Which  means, that blocking operators will bring the whole pipeline to a halt.
    """

    def __init__(self, topology: SequentialTopology[Result, Out], stop: threading.Event = None,
                 logger=logging.getLogger(__name__)):
        """
        Initializes a pipe

        :param stop: in case the event is set, the env will stop executing and close the source and sink
        """
        super(SequentialPipe, self).__init__(topology)
        self.source = self.topology.source
        self.operator = self.topology.operator
        self.sink = self.topology.sink
        self.stop = stop or threading.Event()
        self.logger = logger

    def open(self):
        self.logger.debug('env opens source and sink')
        self.source.open()
        self.sink.open()
        self.operator.open()

    def close(self):
        self.logger.debug('env closes source and sink')
        self.source.close()
        self.sink.close()
        self.operator.close()

    def execute(self, skip_none: bool = True):
        """
        Executes the topology
        In case skip_none is False, None will be passed to the operator and to the sink.
        In case it is set to True, the current iteration will be stopped and a new item will be read
        """
        self.logger.info(f'start executing following topology: {self.pretty_string()}')
        self.open()
        while not self.stop.is_set():
            data: Result = self.source.read()
            if skip_none and data is None:
                self.logger.warning('Source returned None')
                continue

            self.logger.debug(f'process data: {data}')
            data: Out = self.operator.apply(data)
            if skip_none and data is None:
                self.logger.warning('Operator returned None')
                continue

            self.logger.debug(f'data after applying {self.operator.name}: {data}')
            self.logger.debug('write data to sink')
            self.sink.write(data)
        self.close()

    def pretty_string(self):
        return f"{self.source.name} -> {self.operator.pretty_string()} -> {self.sink.name}"
