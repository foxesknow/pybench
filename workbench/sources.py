from typing import AsyncIterable, Any, List

from workbench.core import Source

class NullSource(Source):
    """A source that yields nothing"""
    async def run(self) -> AsyncIterable[Any]:
        yield
    

class NumbersSource(Source):
    """A source that returns a sequence of numbers"""
    def __init__(self, start: int = 0, count: int = 0) -> None:
        super().__init__()
        self.__start = start
        self.__count = count

    async def run(self) -> AsyncIterable[Any]:
        start = self.__start
        count = self.__count
        
        for i in range(start, start+count):
            yield i

class YieldSource(Source):
    """A source that returns a sequence of values"""
    def __init__(self, values: List[Any]) -> None:
        super().__init__()
        self.__values = values

    async def run(self) -> AsyncIterable[Any]:
        values = self.__values
        
        for i in values:
            yield i