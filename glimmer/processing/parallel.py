import logging
import multiprocessing
import threading
from dataclasses import dataclass
from typing import TypeVar, Generic, List, Tuple, Callable

from glimmer.processing import Topology, Operator, Source, Sink, Node, Executable, Environment

Result = TypeVar("Result")
Out = TypeVar("Out")


@dataclass
class ParallelTopology(Topology, Generic[Result, Out]):
    __POISON__ = 'POISON'

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

    @property
    def nodes(self):
        all_nodes = self.operators.copy()
        all_nodes.extend(self.sources)
        all_nodes.extend(self.sinks)
        return all_nodes

    def stop_topology(self):
        for q in self.queues.values():
            q.put(ParallelTopology.__POISON__)



def _contains_duplicate_node(nodes: List[Node], check: Node):
    for node in nodes:
        if node.name == check.name and node is not check:
            return True
    return False


def _warn_duplicate(nodes: List[Node], logger: logging.Logger):
    already_logged = dict()
    for node in nodes:
        if _contains_duplicate_node(nodes, node) and already_logged.get(node.name) is None:
            already_logged[node.name] = True
            logger.warning(f'Topology was initialized twice with the same name "{node.name}".'
                           f'Nodes must have unique names, so check if there are any with the same name.')


def mk_parallel_topology(start: List[Source], logger: logging.Logger = logging.getLogger(__name__)) -> ParallelTopology:
    """
    Helper function to generate a topology from a list of initialized sources.
    Goes through the topology in a breadth-first manner to look for all nodes used.
    """
    sources = start
    _warn_duplicate(sources, logger)
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


def get_items(in_qs):
    items = dict()
    for node_name, in_q in in_qs:
        item = in_q.get()

        if item == ParallelTopology.__POISON__:
            items = ParallelTopology.__POISON__
            return items

        items[node_name] = item
    if len(items) == 1:
        items = list(items.values())[0]
    return items


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
        self.closed = False

    def run(self, stop: multiprocessing.Event):
        self.logger.debug(f'start operator {self.name}')
        self.open()
        try:
            while not stop.is_set():
                items = get_items(self.in_qs)
                if items == ParallelTopology.__POISON__:
                    return
                self.apply(items, self.publish)
        except (KeyboardInterrupt, EOFError):
            return
        finally:
            self.close()

    def publish(self, out):
        # TODO maybe make None filtering optional via parameter
        if out is not None:
            for out_q in self.out_qs:
                out_q.put(out)

    @property
    def logger(self):
        return self.op.logger

    @property
    def name(self):
        return self.op.name

    def open(self):
        self.op.open()

    def apply(self, data, out):
        return self.op.apply(data, out)

    def close(self):
        if not self.closed:
            self.logger.warning(f'Shutting down {self.op.name}')
            self.op.close()
            self.closed = True

    def __str__(self):
        return str(self.op)


class SinkWrapper:

    def __init__(self, sink: Sink, in_qs: List[Tuple[str, multiprocessing.Queue]]):
        if len(in_qs) == 0:
            raise AttributeError(f'Sink does not have any inputs {sink.name}')

        self.sink = sink
        self.in_qs = in_qs
        self.closed = False

    def run(self, stop: multiprocessing.Event):
        self.logger.debug(f'start sink {self.name}')
        self.open()
        try:
            while not stop.is_set():
                items = get_items(self.in_qs)

                if items == ParallelTopology.__POISON__:
                    return

                self.write(items)
        except (KeyboardInterrupt, EOFError):
            pass
        finally:
            self.close()
            return

    def open(self):
        self.sink.open()

    def write(self, data):
        self.sink.write(data)

    def close(self):
        if not self.closed:
            self.closed = True
            self.logger.warning(f'Shutting down {self.sink.name}')
            self.sink.close()

    @property
    def name(self) -> str:
        return self.sink.name

    @property
    def logger(self) -> logging.Logger:
        return self.sink.logger

    def __str__(self):
        return str(self.sink)


class SourceWrapper:

    def __init__(self, source: Source, out_qs: List[multiprocessing.Queue]):
        if len(out_qs) == 0:
            raise AttributeError(f'Source does not contain any outgoing queues {source.name}')
        self.source = source
        self.out_qs = out_qs
        self.closed = False

    def run(self, stop: multiprocessing.Event):
        self.logger.debug(f'start source {self.name}')
        try:
            while not stop.is_set():
                self.read(self.publish)
        except (KeyboardInterrupt, EOFError):
            pass
        finally:
            self.close()

    def read(self, out):
        self.source.read(out)

    def close(self):
        if not self.closed:
            self.closed = True
            self.logger.warning(f'Shutting down {self.name}')
            self.source.close()

    def publish(self, item):
        # TODO maybe make None filtering optional via parameter
        if item is not None:
            for out_q in self.out_qs:
                out_q.put(item)

    @property
    def name(self):
        return self.source.name

    @property
    def logger(self):
        return self.source.logger

    def __str__(self):
        return str(self.source)


class ParallelEnvironment(Environment, Generic[Result, Out]):
    """This environment will execute each node in its own thread. Nodes communicate via multiprocessing.Queue instances
    to publish and receive data. This allows to let each node work at its own pace
    """

    def __init__(self, topology: ParallelTopology,
                 task_factory: Callable[[Node, multiprocessing.Event], Executable], logger: logging.Logger = None):
        """
        Initializes the environment
        :param topology: the topology that will be executed
        :param task_factory: factory function that produces from a node and a stop event an executable,
                            i.e.: multiprocessing.Process or threading.Thread
        :param logger
        """
        super().__init__(topology, multiprocessing.Event())
        if logger is None:
            logger = logging.getLogger(__name__)
        self.task_factory = task_factory
        self.logger = logger
        self.p = None
        self.nodes = topology.nodes

    def start(self, use_thread: bool = False):
        if use_thread:
            self.p = threading.Thread(target=self.run)
        else:
            self.p = multiprocessing.Process(target=self.run)
        self.p.start()

    def join(self, timeout: int = None):
        self.p.join(timeout)

    def run(self):
        processes = []
        for node in self.topology.nodes:
            processes.append(self.task_factory(node, self.stop_signal))

        for p in processes:
            p.start()

        self.logger.warning('Started topology, watiting for stop signal')
        self.stop_signal.wait()
        self.topology.stop_topology()
        self.logger.warning('Received stop signal, stopping all processes')

    def stop(self):
        self.logger.info('Stop environment')
        self.stop_signal.set()

    def close(self):
        for node in self.nodes:
            node.close()
