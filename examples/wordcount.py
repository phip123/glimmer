import pypeline.processing.parallel as pr
from examples.wordcount.nodes import WordsSource, FlatmapLines, MapWords, Reducer


def main():
    print('Wordcount example')
    """
    Topology
    
    words -> separate-lines -> filter-empty -> flatmap-lines -> map-words -> reducer -> printing-sink 
    """
    words = WordsSource()
    separate_lines = pr.mk_op(lambda x: x.split(' '), 'separate-lines')
    filter_empty = pr.mk_op(lambda x: x if len(x) > 0 else None)
    flatmap = FlatmapLines()
    map_words = MapWords()
    reducer = Reducer()
    sink = pr.mk_sink(print, 'printing-sink')
    top = (words
           | separate_lines
           | flatmap
           | filter_empty
           | map_words
           | reducer
           | sink
           )

    top = pr.mk_parallel_topology([words])
    pipe = pr.mk_env(top)
    pipe.execute()


if __name__ == '__main__':
    main()
