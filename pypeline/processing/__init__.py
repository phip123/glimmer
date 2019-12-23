import abc
import typing
from typing import TypeVar, Generic

from pypeline.util import Infix
from pypeline.util.context import Context

Result = TypeVar("Result")
In = TypeVar("In")
Out = TypeVar("Out")
A = TypeVar("A")
B = TypeVar("B")
C = TypeVar("C")


class EnvironmentExecutionError(Exception):
    """Exception raised for errors that happened during executing an environment
    """
    pass


class InitializationError(EnvironmentExecutionError):
    """Exception raised in case opening a source or sink fails
    Attributes:
        name -- name of source/sink
        message -- explanation of the error
    """

    def __init__(self, name: str, message):
        self.name = name
        self.message = message


class ShutdownError(EnvironmentExecutionError):
    """Exception raised in case closing a source or sink fails
    Attributes:
        name -- name of source/sink
        message -- explanation of the error
    """

    def __init__(self, name: str, message):
        self.name = name
        self.message = message


class OperatorError(EnvironmentExecutionError):
    """Exception raised in case an operator fails
    Attributes:
        name -- name of operator
        message -- explanation of the error
    """

    def __init__(self, name: str, message):
        self.name = name
        self.message = message


class ReadError(EnvironmentError):
    """Exception raised in case reading from source fails
    Attributes:
        name -- name of source
        message -- explanation of the error
    """

    def __init__(self, name: str, message):
        self.name = name
        self.message = message


class WriteError(EnvironmentError):
    """Exception raised in case writing into sink fails
    Attributes:
        name -- name of sink
        message -- explanation of the error
    """

    def __init__(self, name: str, message):
        self.name = name
        self.message = message


class Node(abc.ABC):
    name: str

    def __init__(self, ctx: Context=None) -> None:
        super().__init__()
        self.ctx = ctx or Context(config_name=self.name)
        self.logger = ctx.create_logger(self.name)

    def open(self):
        """Opens any necessary connections
        :raises:
            InitializationError
        """
        pass

    def close(self):
        """Closes all remaining connections
        :raises:
            ShutdownError
        """
        pass


class Source(Node, Generic[Result]):

    def read(self) -> Result:
        """Reads from the source, type of return value depends on implementation
        :raises:
            ReadError
        :return: data read from the source
        """
        raise NotImplementedError


class Sink(Node, Generic[In]):

    def write(self, data: In):
        """Consumes data
        :raises:
            WriteError
        :return: no return value
        """
        raise NotImplementedError


class Operator(Node, Generic[In, Out]):

    def apply(self, data: In) -> Out:
        """Consumes data, possible transforms it, and returns data
        :raises:
            OperatorError: in case there is an error during application
        :return: transformed data, depends on implementation
        """
        raise NotImplementedError

    def __sub__(self, other):
        return composition(self, other)

    def pretty_string(self) -> str:
        return self.name


class Topology:
    pass


class Environment:
    """An environment repeatedly executes its topology
    """

    def __init__(self, topology: Topology):
        self.topology = topology

    def execute(self):
        """Executes the topology
        :raises:
            EnvironmentExecutionError: in case there was an error opening, executing or closing the topology
        """
        raise NotImplementedError


def composition(op_1: Operator[A, B], op_2: Operator[B, C], fail_fast: bool = True) -> Operator[A, C]:
    """
    Returns the composition of the passed functions.
    The fail_fast parameter dictates how the case is handled if the first operator returns None.
    If it is set to true, the second function will not be called.
    """

    def f(x):
        value = op_1.apply(x)
        if fail_fast and value is None:
            return None
        return op_2.apply(value)

    class ComposedOperator(Operator[A, C]):
        name = f"({op_1.name} -> {op_2.name})"

        def apply(self, data: A) -> C:
            return f(data)

        def open(self):
            op_1.open()
            op_2.open()

        def close(self):
            op_1.close()
            op_2.close()

    return ComposedOperator()


compose = Infix(composition)


def compose_list(operators: typing.List[Operator]) -> Operator:
    if len(operators) == 1:
        return operators[0]
    elif len(operators) == 0:
        raise AssertionError("No operator in list")
    else:
        composed = operators[0] | compose | operators[1]
        for op in operators[2:]:
            composed = composed | compose | op
        return composed
