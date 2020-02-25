from time import sleep
from typing import Callable, List, Tuple

from glimmer.processing import Source, Operator
from glimmer.util.context import Context


class Reducer(Operator[Tuple[str, int], Tuple[str, int]]):
    name = 'reducer'
    """
    Stateful operator
    """

    def __init__(self, ctx: Context = None) -> None:
        super().__init__(ctx)
        self.statistics = dict()

    def apply(self, data: Tuple[str, int], out: Callable[[Tuple[str, int]], None]):
        count = self.statistics.get(data[0], 0)
        count += data[1]
        self.statistics[data[0]] = count
        out((data[0], count))


class MapWords(Operator[str, Tuple[str, int]]):
    name = 'map-words'

    def apply(self, data: str, out: Callable[[Tuple[str, int]], None]):
        out((data, 1))


class FlatmapLines(Operator[List[str], str]):
    name = 'flatmap-lines'

    def apply(self, data: List[str], out: Callable[[str], None]):
        for d in data:
            out(d)


class WordsSource(Source[str]):
    name = 'words'

    lines = """
        Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore 
        magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd 
        gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing 
        elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero 
        eos  et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem
        ipsum dolor sit amet.
        """.splitlines()

    def __init__(self, ctx: Context = None) -> None:
        super().__init__(ctx)
        self.idx = 0

    def open(self):
        print(f'Open file: {self.ctx.getenv("file")}')

    def read(self, out: Callable[[str], None]):
        if self.idx < len(self.lines):
            out(self.lines[self.idx])
            self.idx += 1
            sleep(1)
