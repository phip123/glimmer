import multiprocessing

import pypeline.processing.factory as factory
from examples.wordcount.nodes import WordsSource, FlatmapLines, MapWords, Reducer
from pypeline.util.context import Context


def main():
    print('Wordcount example')
    """
    Topology
    
    words -> separate-lines -> filter-empty -> flatmap-lines -> map-words -> reducer -> printing-sink 
    """

    # For passing values to nodes, you have to use the config object
    source_config = {
        'file': 'taxi_data.csv'
    }

    words = WordsSource(Context(config=source_config))
    separate_lines = factory.mk_op(lambda x: x.split(' '), 'separate-lines')
    filter_empty = factory.mk_op(lambda x: x if len(x) > 0 else None)
    flatmap = FlatmapLines()
    map_words = MapWords()
    reducer = Reducer()
    sink = factory.mk_sink(print, 'printing-sink')

    # Connect nodes
    (words
     | separate_lines
     | flatmap
     | filter_empty
     | map_words
     | reducer
     | sink
     )

    stop = multiprocessing.Event()

    # We can deduce from the source all other nodes
    # This creates a topology that will be executed synchronously
    env = factory.mk_synchronous_env(words, stop)

    # In case you would like to have all nodes executed in separated threads or processes:
    # env = factory.mk_parallel_env([words], stop=stop)
    try:
        env.run()
    except KeyboardInterrupt:
        stop.set()
    print('Goodbye!')


if __name__ == '__main__':
    main()
