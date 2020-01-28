import logging
import multiprocessing
import threading
from dataclasses import dataclass
from typing import TypeVar, Generic, List, Tuple, Callable

from pypeline.processing import Topology, Operator, Source, Sink, Node, Executable

Result = TypeVar("Result")
Out = TypeVar("Out")


@dataclass
class ParallelTopology(Topology, Generic[Result, Out]):

    def __init__(self, sources, operators, sinks):
        self.raw_sources = sources
        self.raw_operators = operators
        self.raw_sinks = sinks
        self.sources = []
        self.operators = []
        self.sinks = []
        self.queues = dict()
        self._prepare_topology()

    def _prepare_topology(self):
        sources = []
        for source in self.raw_sources:
            queues = []
            for out in list(source.outputs.keys()):
                queue = multiprocessing.Queue()
                queues.append(queue)
                self.queues[(source.name, out)] = queue

            sources.append(SourceWrapper(source, queues))

        for operator in self.raw_operators:
            queues = []
            for out in list(operator.outputs.keys()):
                queue = multiprocessing.Queue()
                queues.append(queue)
                self.queues[(operator.name, out)] = queue

        operators = []
        for operator in self.raw_operators:
            in_queues = []
            for node in list(operator.inputs.keys()):
                queue = self.queues.get((node, operator.name))
                if queue is None:
                    raise AttributeError(f'Found uninitialized edge: {node}->{operator.name}')
                in_queues.append((node, queue))
            out_queues = []
            for node1, node2 in self.queues:
                if node1 == operator.name:
                    out_queues.append(self.queues[(node1, node2)])

            operators.append(OperatorWrapper(operator, in_qs=in_queues, out_qs=out_queues))

        sinks = []
        for sink in self.raw_sinks:
            queues = []
            for node in list(sink.inputs.keys()):
                queue = self.queues.get((node, sink.name))
                if queue is None:
                    raise AttributeError(f'Found uninitialized edge: {node}->{sink.name}')
                queues.append((node, queue))
            sinks.append(SinkWrapper(sink, queues))

        self.sources = sources
        self.operators = operators
        self.sinks = sinks


def mk_parallel_topology(start: List[Source]) -> ParallelTopology:
    """
    Helper function to generate a topology from a list of initialized sources.
    Goes through the topology in a breadth-first manner to look for all nodes used.
    """
    sources = start
    operators = dict()
    sinks = dict()

    def register_node(node):
        if isinstance(node, Operator):
            operators[node.name] = node
            for out in node.outputs.values():
                register_node(out)
        elif isinstance(node, Sink):
            sinks[node.name] = node
        elif isinstance(node, Source):
            for out in node.outputs.values():
                register_node(out)
        else:
            raise AttributeError('Unknown Node Type encountered')

    for source in sources:
        register_node(source)

    return ParallelTopology(sources, list(operators.values()), list(sinks.values()))


class OperatorWrapper:

    def __init__(self, op: Operator, in_qs: List[Tuple[str, multiprocessing.Queue]],
                 out_qs: List[multiprocessing.Queue]):
        if len(in_qs) == 0:
            raise AttributeError(f'Operator does not have any inputs {op.name}')
        if len(out_qs) == 0:
            raise AttributeError(f'Operator does not have any outputs {op.name}')

        self.op = op
        self.in_qs = in_qs
        self.out_qs = out_qs

    def run(self, stop: threading.Event):
        self.op.logger.debug(f'start operator {self.op.name}')
        self.op.open()
        while not stop.is_set():
            try:
                items = dict()
                for node_name, in_q in self.in_qs:
                    in_item = in_q.get()
                    items[node_name] = in_item

                if len(items) == 1:
                    items = list(items.values())[0]

                def publish(out):
                    # TODO maybe make None filtering optional via parameter
                    if out is not None:
                        for out_q in self.out_qs:
                            out_q.put(out)

                self.op.apply(items, publish)
            except KeyboardInterrupt:
                self.op.logger.warning(f'Shutting down {self.op.name}')
                self.op.close()
                return

    def apply(self, data, out):
        return self.op.apply(data, out)


class SinkWrapper:

    def __init__(self, sink: Sink, in_qs: List[Tuple[str, multiprocessing.Queue]]):
        if len(in_qs) == 0:
            raise AttributeError(f'Sink does not have any inputs {sink.name}')

        self.sink = sink
        self.in_qs = in_qs

    def run(self, stop: threading.Event):
        self.sink.logger.debug(f'start sink {self.sink.name}')
        self.sink.open()
        while not stop.is_set():
            try:
                items = dict()
                for node_name, in_q in self.in_qs:
                    item = in_q.get()
                    items[node_name] = item

                if len(items) == 1:
                    items = list(items.values())[0]

                self.sink.write(items)
            except KeyboardInterrupt:
                self.sink.logger.warning(f'Shutting down {self.sink.name}')
                self.sink.close()
                return

    def write(self, data):
        self.sink.write(data)


class SourceWrapper:

    def __init__(self, source: Source, out_qs: List[multiprocessing.Queue]):
        if len(out_qs) == 0:
            raise AttributeError(f'Source does not contain any outgoing queues {source.name}')
        self.source = source
        self.out_qs = out_qs

    def run(self, event: multiprocessing.Event):
        self.source.logger.debug(f'start source {self.source.name}')
        while not event.is_set():
            try:
                def publish(item):
                    # TODO maybe make None filtering optional via parameter
                    if item is not None:
                        for out_q in self.out_qs:
                            out_q.put(item)

                self.read(publish)
            except KeyboardInterrupt:
                self.source.logger.warning(f'Shutting down {self.source.name}')
                self.source.close()
                return

    def read(self, out):
        self.source.read(out)


class ParallelEnvironment(Generic[Result, Out]):
    """This environment will execute each node in its own thread. Nodes communicate via multiprocessing.Queue instances
    to publish and receive data. This allows to let each node work at its own pace
    """

    def __init__(self, topology: ParallelTopology, stop: threading.Event,
                 task_factory: Callable[[Node, threading.Event], Executable], logger: logging.Logger = None):
        """
        Initializes the environment
        :param topology: the topology that will be executed
        :param stop: if this event is, the pipe will stop processing and close all nodes
        :param task_factory: factory function that produces from a node and a stop event an executable,
                            i.e.: multiprocessing.Process or threading.Thread
        :param logger
        """
        if logger is None:
            logger = logging.getLogger(__name__)
        self.topology = topology
        self.task_factory = task_factory
        self.logger = logger
        self.stop = stop

    def execute(self):
        processes = []
        stops = []
        all_nodes = self.topology.operators
        all_nodes.extend(self.topology.sources)
        all_nodes.extend(self.topology.sinks)

        for node in all_nodes:
            stop = threading.Event()
            processes.append(self.task_factory(node, stop))
            stops.append(stop)

        for p in processes:
            p.start()

        self.logger.debug('Started topology')
        self.stop.wait()
        self.logger.debug('Received stop signal, stopping all processes')
        for stop, p in zip(stops, processes):
            stop.set()
            p.join(timeout=5)
