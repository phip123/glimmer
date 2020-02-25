glimmer
---------------

glimmer is a library that helps developers build processing pipelines that do not have the need to be computed in a distributed fashion but are easy and fast to setup and still profit of the
declerative way of designing  processing pipelines and the flexibility of combining different sources, sinks and operators.
    
    
API
---
Pypeline consists of the following basic blocks:
* `Nodes` are the processing units and there are three different types:
    1. `Source`: Nodes that generate data to process
    2. `Operator`: Nodes that receive and output data
    3. `Sink`: Nodes that only consume incoming data. 
 
* `Topology` is the internal representation for the dataflow and represents a [DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph),
which describes the way nodes interact with each other. 
* `Environments` execute a given topology. 

`glimmer` currently offers two modes of execution: `synchronous` and `parallel`.
 
* In `synchronous` mode there can be only **one** source, **one** sink and multiple operators between.
* The `parallel` mode supports every valid DAG, which means: multiple sources, multiple sinks, joins, merges, ...

The following part will show real quick to get either of them running.

`Synchronous` mode first (you can find this also under `examples/readme/synchronous.py`):
This example just reverses the string 'hello, world!' and prints it.
To keep it short we use the function-based approach, which has its drawbacks but is sufficient for
stateless operations
```python
import glimmer.processing.factory as factory

hello_source = factory.mk_src(lambda: 'hello')
world_op = factory.mk_op(lambda x: f'{x}, world!')
rev_op = factory.mk_op(lambda x: x[::-1])
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
env.close()

```
You can get a synchronous environment up rather quickly thanks to some factory functions.
In case you want more sophisticated nodes, want to use a logger, potentially your own, you need to checkout our examples
located in `examples`. Most notable is that if you decide to go with our class-based approach, you can keep track of state 
and use `Context` to easily access env-variables and pass custom properties to the nodes.

Next up is the `parallel` environment which does the same as the synchronous one, but uses multiple sources.
(you can find this also under `examples/readme/parallel.py`):
```python
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

``` 
If you run the code, you will see that the `combine_op` will wait for both inputs. This means that the order of outputs is kept
over the complete topology.

##### Context    
An important aspect for us is the configurability of each individual nodes.

To allow the desired level of configurability, we created the `Context`. 
This class is used for providing each node with 
environment variables, which can be OS env variables or loaded from a .yaml file. This allows us to open on every node a different connection to for example redis.


Examples
--------
More examples for using `glimmer` are located in the `examples` folder.

Build
-----

Install with make

    make venv

*or* create and activate a new virtual environment

    virtualenv .venv
    source .venv/bin/activate

Install requirements

    pip install -r requirements.txt
    
For running tests you need to run additionally
    
    pip install -r requirements-dev.txt
