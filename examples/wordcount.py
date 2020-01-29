import threading

import pypeline.processing.factory as factory
from examples.wordcount.nodes import WordsSource, FlatmapLines, MapWords, Reducer


def main():
    print('Wordcount example')
    """
    Topology
    
    words -> separate-lines -> filter-empty -> flatmap-lines -> map-words -> reducer -> printing-sink 
    """
    words = WordsSource()
    separate_lines = factory.mk_op(lambda x: x.split(' '), 'separate-lines')
    filter_empty = factory.mk_op(lambda x: x if len(x) > 0 else None)
    flatmap = FlatmapLines()
    map_words = MapWords()
    reducer = Reducer()
    sink = factory.mk_sink(print, 'printing-sink')
    (words
     | separate_lines
     | flatmap
     | filter_empty
     | map_words
     | reducer
     | sink
     )

    stop = threading.Event()
    env = factory.mk_parallel_env([words], stop=stop)
    try:
        env.execute()
    except KeyboardInterrupt:
        stop.set()
    print('Goodbye!')


if __name__ == '__main__':
    main()
