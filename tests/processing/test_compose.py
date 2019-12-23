import unittest

from pypeline.processing import Operator, In, Out, composition


class Op(Operator):
    name = 'Op'

    def apply(self, data: In) -> Out:
        return data


class CompositionTest(unittest.TestCase):

    def test_composition(self):
        op1 = Op()
        op2 = Op()
        composed = composition(op1, op2)
        self.assertEqual('asdf', composed.apply('asdf'))
