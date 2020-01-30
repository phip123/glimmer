import abc
from typing import TypeVar, Generic, Dict, List, Callable

from pypeline.util.context import Context

Result = TypeVar("Result")
In = TypeVar("In")
Out = TypeVar("Out")
A = TypeVar("A")
B = TypeVar("B")
C = TypeVar("C")


class InvalidTopologyError(Exception):
    """Exception raised in case the topology is invalid
    """

    def __init__(self, message):
        self.message = message


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


class ReadError(EnvironmentExecutionError):
    """Exception raised in case reading from source fails
    Attributes:
        name -- name of source
        message -- explanation of the error
    """

    def __init__(self, name: str, message):
        self.name = name
        self.message = message


class WriteError(EnvironmentExecutionError):
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
    inputs: Dict[str, 'Node']
    outputs: Dict[str, 'Node']

    def __init__(self, ctx: Context = None) -> None:
        super().__init__()
        self.ctx = ctx or Context(config_name=self.name)
        self.logger = self.ctx.create_logger(self.name)
        self.inputs = dict()
        self.outputs = dict()

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

    def send_to(self, other):
        """
        Adds passed nodes or functions as output receiving nodes
        """
        self._add(other, self._register_output_node, self._register_output_function)
        return other

    def receive_from(self, other):
        """
        Adds passed nodes or functions as as input for this node
        """
        self._add(other, self._register_input_node, self._register_input_function)
        return self

    def __gt__(self, other):
        """
        Adds passed nodes or functions as output receiving nodes
        """
        return self.send_to(other)

    def __lt__(self, other):
        """
        Adds passed nodes or functions as as input for this node
        """
        return self.receive_from(other)

    def __or__(self, other):
        """
        Adds passed nodes or functions as output receiving nodes
        """
        return self.send_to(other)

    @staticmethod
    def _add(other, register_node, register_func):
        if isinstance(other, List):
            for n in other:
                if isinstance(n, Callable):
                    register_func(n)
                elif isinstance(n, Node):
                    register_node(n)
                else:
                    raise AttributeError('Argument not supported as receiver')
        elif isinstance(other, Node):
            register_node(other)
        elif isinstance(other, Callable):
            register_func(other)
        else:
            raise AttributeError('Argument not supported as receiver')

    def _register_input_node(self, node: 'Node'):
        self.inputs[node.name] = node
        node.outputs[self.name] = self

    def _register_input_function(self, f):
        # TODO implement
        raise NotImplementedError

    def _register_output_function(self, node):
        # TODO implement
        raise NotImplementedError

    def _register_output_node(self, node: 'Node'):
        self.outputs[node.name] = node
        node.inputs[self.name] = self


class Source(Node, Generic[Result]):

    def read(self, out: Callable[[Result], None]):
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

    def apply(self, data: In, out: Callable[[Out], None]):
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

    class ComposedOperator(Operator[A, C]):
        name = f"({op_1.name} -> {op_2.name})"

        def apply(self, data: A, out: Callable[[C], None]):
            def out_f(data_f):
                if not (fail_fast and data_f is None):
                    op_2.apply(data_f, out)

            op_1.apply(data, out_f)

        def open(self):
            op_1.open()
            op_2.open()

        def close(self):
            op_1.close()
            op_2.close()

    return ComposedOperator()


def compose_list(operators: List[Operator]) -> Operator:
    if len(operators) == 1:
        return operators[0]
    elif len(operators) == 0:
        raise AssertionError("No operator in list")
    else:
        composed = operators[0] - operators[1]
        for op in operators[2:]:
            composed = composed - op
        return composed


class Executable(abc.ABC):
    """Represents a joinable process which starts a node
    """

    def start(self):
        raise NotImplementedError

    def join(self, timeout):
        raise NotImplementedError
