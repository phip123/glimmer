import logging
import threading
from dataclasses import dataclass
from typing import TypeVar, Generic

from pypeline.processing import Topology, Environment

Result = TypeVar("Result")
Out = TypeVar("Out")


@dataclass
class ParallelTopology(Topology, Generic[Result, Out]):
    """

    """
    pass


class ParallelPipe(Environment, Generic[Result, Out]):
    """This pipe will execute each node in its own thread. Nodes communicate via multiprocessing.Queue instances
    to publish and receive data. This allows to let each node work at its own pace
    """

    def __init__(self, topology: ParallelTopology[Result, Out], stop: threading.Event=None, logger=logging.getLogger(__name__)):
        """
        Initializes the pipe
        :param topology: the topology that will be executed
        :param stop: if this event is, the pipe will stop processing and close all nodes
        :param logger
        """
        super(ParallelPipe, self).__init__(topology)
        self.logger = logger
        self.stop = stop

