import unittest
from unittest.mock import Mock

from glimmer.processing import Operator, In, composition


class Op(Operator):
    name = 'Op'

    def apply(self, data: In, out):
        out(data)


class CompositionTest(unittest.TestCase):

    def test_composition(self):
        op1 = Op()
        op2 = Op()
        composed = composition(op1, op2)
        mock = Mock()
        composed.apply('asdf', mock)
        mock.assert_called_with('asdf')
