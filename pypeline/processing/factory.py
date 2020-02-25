import logging
import multiprocessing
import threading
from typing import List

from pypeline.processing import Node, Source, Sink, Operator, Executable
from pypeline.processing.parallel import ParallelEnvironment, mk_parallel_topology
from pypeline.processing.sync import SynchronousEnvironment, mk_synchronous_topology
from pypeline.util import generate_node_name


def mk_src(func, node_name: str = None) -> Node:
    if node_name is None:
        node_name = f'source-{generate_node_name()}'

    class FuncSource(Source):
        name = node_name

        def read(self, out):
            out(func())

    return FuncSource()


def mk_sink(func, node_name: str = None) -> Node:
    if node_name is None:
        node_name = f'sink-{generate_node_name()}'

    class FuncSink(Sink):
        name = node_name

        def write(self, data):
            func(data)

    return FuncSink()


def mk_op(func, node_name: str = None) -> Node:
    if node_name is None:
        node_name = f'op-{generate_node_name()}'

    class FuncOp(Operator):
        name = node_name

        def apply(self, data, out):
            out(func(data))

    return FuncOp()


def process_factory():
    def factory(node, stop):
        process = multiprocessing.Process(target=node.run, args=(stop,))

        class ProcessExecutable(Executable):

            def start(self):
                process.start()

            def join(self, timeout):
                process.join(timeout)

        return ProcessExecutable()

    return factory


def thread_factory():
    def factory(node, stop):
        thread = threading.Thread(target=node.run, args=(stop,))

        class ThreadExecutable(Executable):

            def start(self):
                thread.start()

            def join(self, timeout):
                thread.join(timeout)

        return ThreadExecutable()

    return factory


def mk_parallel_env(sources: List[Source], logger: logging.Logger = None, task_factory=None) -> ParallelEnvironment:
    if task_factory is None:
        task_factory = process_factory()

    top = mk_parallel_topology(sources)
    return ParallelEnvironment(top, task_factory, logger)


def mk_synchronous_env(source: Source) -> SynchronousEnvironment:
    top = mk_synchronous_topology(source)
    return SynchronousEnvironment(top)
