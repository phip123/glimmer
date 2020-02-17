import logging
import multiprocessing
import threading
import time
from typing import List

from pypeline.processing import Node, Source, Sink, Operator, Executable
from pypeline.processing.parallel import ParallelEnvironment, mk_parallel_topology
from pypeline.processing.sync import SynchronousEnvironment, mk_synchronous_topology


def mk_src(func, node_name: str = None) -> Node:
    if node_name is None:
        node_name = f'source-{str(time.time_ns())[5:-5]}'

    class FuncSource(Source):
        name = node_name

        def read(self, out):
            out(func())

    return FuncSource()


def mk_sink(func, node_name: str = None) -> Node:
    if node_name is None:
        node_name = f'sink-{str(time.time_ns())[5:-5]}'

    class FuncSink(Sink):
        name = node_name

        def write(self, data):
            func(data)

    return FuncSink()


def mk_op(func, node_name: str = None) -> Node:
    if node_name is None:
        node_name = f'op-{str(time.time_ns())[5:-5]}'

    class FuncOp(Operator):
        name = node_name

        def apply(self, data, out):
            out(func(data))

    return FuncOp()


def process_factory():
    def factory(node):
        process = multiprocessing.Process(target=node.run)

        class ProcessExecutable(Executable):

            def start(self):
                process.start()

            def join(self, timeout):
                process.join(timeout)

        return ProcessExecutable()

    return factory


def thread_factory():
    def factory(node):
        thread = threading.Thread(target=node.run)

        class ThreadExecutable(Executable):

            def start(self):
                thread.start()

            def join(self, timeout):
                thread.join(timeout)

        return ThreadExecutable()

    return factory


def mk_parallel_env(sources: List[Source], stop: multiprocessing.Event = None,
                    logger: logging.Logger = None, task_factory=None) -> ParallelEnvironment:
    if task_factory is None:
        task_factory = process_factory()

    if stop is None:
        stop = multiprocessing.Event()
    top = mk_parallel_topology(sources)
    return ParallelEnvironment(top, stop, task_factory, logger)


def mk_synchronous_env(source: Source, stop: multiprocessing.Event = None) -> SynchronousEnvironment:
    if stop is None:
        stop = multiprocessing.Event()
    top = mk_synchronous_topology(source)
    return SynchronousEnvironment(top, stop)
