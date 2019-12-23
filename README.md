pypeline
---------------

pypeline is a library that helps developers build processing pipelines that do not have the need to be computed in a distributed fashion but are easy and fast to setup and still profit of the
declerative way of designing  processing pipelines and the flexibility of combining different sources, sinks and operators.
    
    
API
---
Pypeline consists of the following basic blocks:
* `Pipe`: Is built by combining different nodes to form a pipeline of data transformations, starting from retrieving data and terminating with a singe node. 
Pipes are directed acyclic graphs.
* `Nodes` are the processing units and there are three different types:
    1. `Source`: Nodes that generate data to process
    2. `Operator`: Nodes that receive and output data
    3. `Sink`: Nodes that are the last element of a pipe. 


Nodes can implement an `open` and `close` method to acquire and release resources when initializing and exiting the pipe.

A pipe is given upon construction a `Topology`, which represents the graph. The pipe is responsible for executing the topology,
currently there is only a `SynchronousPipe` available, which takes a `SynchronousTopology`. This topology consists of exactly:
one `Source`, one `Operator` and one `Sink`. Because `Operators` are just functions, they are composable, which means you can
compose multiple `Operators` to a single one. For this we provide helper functions for ease of use. See: `pypipe.processing.composition` and `pypipe.processing.compose_list`.
For allowing a more concise syntax, you can write: 

    composed = op1 - op2 - op3

##### Context    
An important aspect for us is the configurability of each individual nodes.

To allow the desired level of configurability, we created the `Context`. 
This class is used for providing each node with 
environment variables, which can be OS env variables or loaded from a .yaml file. This allows us to open on every node a different connection to for example redis.

##### Registry

There is also a global registry, where you can register your nodes to allow a dynamic pipe generation. This means
that you can start a pipe by only passing your node names to the `pypipe.daemon`, which can build your pipe and execute it.

This is especially useful in case you want to define your pipe via cli arguments to easily configure your pipeline.
The general approach is to register all your nodes and then pass the name of your source, sink and operators to the daemon.
 

##### CLI
The aforementioned registry allows us to provide you with a ready-to-use CLI application which will parse your arguments
and  start the pipe. See `examples.cli`  on how to build such application.


Examples
--------
Examples for using `pypeline` are located in the `examples` folder.

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
