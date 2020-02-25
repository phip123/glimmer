import glimmer.processing.factory as factory

# Create sink that just emits 'hello', sinks get called repeatedly after the sink consumed its input
hello_source = factory.mk_src(lambda: 'hello')

# Create operator that appends ', world!' to its input
world_op = factory.mk_op(lambda x: f'{x}, world!')

# Create operator that reverses its input
rev_op = factory.mk_op(lambda x: x[::-1])

# Create a sink that just prints its input
print_sink = factory.mk_sink(lambda x: print(x))

# Connect the nodes, in glimmer each nodes keeps track of its in- and outputs
# The '|' operator modifies the state of each node
(hello_source | world_op | rev_op | print_sink)

# Because we can step through the whole graph with one node, we made a helper function to
# generate a read-to-use environment, which also generates the topology for us
env = factory.mk_synchronous_env(hello_source)

# Start the environment in its own process
env.start()

# Hit enter to stop and close env
input()
env.stop()

