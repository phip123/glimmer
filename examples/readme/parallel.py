from time import sleep

import glimmer.processing.factory as factory

world_idx = -1
hello_idx = -1


def world():
    sleep(0.1)
    global world_idx
    world_idx += 1
    return f', world! - {world_idx}'


def hello():
    sleep(1)
    global hello_idx
    hello_idx += 1
    return f'{hello_idx} - hello'


hello_source = factory.mk_src(hello, node_name='hello')
world_source = factory.mk_src(world, node_name='world')

# In case a node receives multiple nodes, it gets a dict that contains each nodes output
# To access a node's output just use its name as key.
combine_op = factory.mk_op(lambda x: x[hello_source.name] + x[world_source.name], node_name='combine')

rev_op = factory.mk_op(lambda x: x[::-1], node_name='rev')
print_sink = factory.mk_sink(lambda x: print(x), node_name='print')
print_len_sink = factory.mk_sink(lambda x: f'Length: {print(len(x))}', node_name='print_len')

# While the '|' operator takes the left nodes output and sets it as the right ones input
# We provide methods that cover the opposite direction, this is helpful in cases where
# you want to provide one node with multiple inputs
combine_op.receive_from([hello_source, world_source])

# Because nodes keep track of their in- and outputs, we can continue to connect nodes starting
# from the 'combine_op' node
# It's also possible to pass a list of nodes to the '|' operator
(combine_op | rev_op | [print_sink, print_len_sink])

# As in the synchronous example it is enough to pass all sources to generate an environment
env = factory.mk_parallel_env([hello_source, world_source])

# Start the environment in its own process
env.start()

# Hit enter to stop and close env
input()
env.stop()
