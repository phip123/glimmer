import logging
import multiprocessing
import threading
from dataclasses import dataclass
from typing import TypeVar, Generic

from pypeline.processing import Source, Operator, Sink, Environment, Topology, InvalidTopologyError, compose_list

Result = TypeVar("Result")
Out = TypeVar("Out")


@dataclass
class SynchronousTopology(Topology, Generic[Result, Out]):
    """
    source: the source the env will read from
    operator: the operator the pipe will use for transforming the data
    """
    source: Source[Result]
    operator: Operator[Result, Out]
    sink: Sink[Out]


def mk_synchronous_topology(source: Source):
    curr = source
    operators = []
    while len(curr.outputs) > 0:
        if len(curr.outputs) != 1:
            raise InvalidTopologyError(
                f'Nodes in synchronous topologies can only have one output, was: {len(curr.outputs)}')
        else:
            curr = list(curr.outputs.values())[0]
            if isinstance(curr, Operator):
                operators.append(curr)
    sink = curr
    composed = compose_list(operators)
    return SynchronousTopology(source, composed, sink)


class SynchronousEnvironment(Environment, Generic[Result, Out]):
    """
    This pipe will call each operator one after another without any kind of parallelism.
    Which  means, that blocking operators will bring the whole pipeline to a halt.
    """

    def __init__(self, topology: SynchronousTopology[Result, Out],
                 logger=logging.getLogger(__name__),
                 skip_none: bool = False):
        """
        Initializes an environment

        """
        super(SynchronousEnvironment, self).__init__(topology)
        self.source = self.topology.source
        self.operator = self.topology.operator
        self.sink = self.topology.sink
        self.logger = logger
        self.skip_none = skip_none
        self.p = None

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

    def start(self, use_thread: bool = False):
        if use_thread:
            self.p = threading.Thread(target=self.run, args=(stop,))
        else:
            self.p = multiprocessing.Process(target=self.run, args=(stop,))

        self.p.start()

    def run(self):
        """
            Executes the topology
            In case skip_none is False, None will be passed to the operator and to the sink.
            In case it is set to True, the current iteration will be stopped and a new item will be read
            """
        self.logger.info(f'start executing following topology: {self.pretty_string()}')
        skip_none = self.skip_none
        self.open()
        while not stop.is_set():

            def op_out(data):
                if skip_none and data is None:
                    self.logger.warning('Operator returned None')

                self.logger.debug(f'data after applying {self.operator.name}: {data}')
                self.logger.debug('write data to sink')
                self.sink.write(data)

            def read_out(data):
                if skip_none and data is None:
                    self.logger.warning('Source returned None')

                self.logger.debug(f'process data: {data}')
                self.operator.apply(data, op_out)

            self.source.read(read_out)
        self.logger.info('Close topology')
        self.close()

    def pretty_string(self):
        return f"{self.source.name} -> {self.operator.pretty_string()} -> {self.sink.name}"
