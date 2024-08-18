from typing import List, AsyncIterator, Any, Optional, Callable
import abc

class Runnable(abc.ABC): 
    pass

class Source(Runnable):
    @abc.abstractmethod
    def run(self) -> AsyncIterator[Any]:
        pass
    

class Filter(Runnable):
    @abc.abstractmethod
    def run(self, input: AsyncIterator[Any]) -> AsyncIterator[Any]:
        pass

class Job(Runnable): 
    @abc.abstractmethod
    async def run(self) -> None:
        pass
    
class _PipelineStart:
    def __init__(self, outer: Callable[[], AsyncIterator[Any]]) -> None:
        self.__outer = outer
    
    async def __call__(self, *args: Any, **kwds: Any) -> AsyncIterator[Any]:
        outer = self.__outer

        async for i in outer():
            yield i

class _PipelineStep:
    def __init__(self, outer: Callable[[], AsyncIterator[Any]], filter: Filter) -> None:
        self.__outer = outer
        self.__filter = filter

    async def __call__(self, *args: Any, **kwds: Any) -> AsyncIterator[Any]:
        outer = self.__outer
        filter = self.__filter

        async for i in filter.run(outer()):
            yield i
    
    
class PipelineJob(Job):
    def __init__(self) -> None:
        super().__init__()
        self.__filters: List[Filter] = []
        self.__source: Optional[Source] = None

    @property
    def source(self) -> Optional[Source]:
        return self.__source
        
    @source.setter
    def source(self, source: Optional[Source]):
        self.__source = source
    

    def add_filter(self, filter: Filter) -> None:
        self.__filters.append(filter)

    async def run(self) -> None:
        factory = self._build_pipeline()
        sequence = factory()

        async for _ in sequence:
            pass
    
    
    def _build_pipeline(self) -> Callable[[], AsyncIterator[Any]]:
        src = self.__source
        if src is None:
            raise ValueError("no source set")

        function = _PipelineStart(lambda: src.run())

        for filter in self.__filters:
            next = _PipelineStep(function, filter)
            function = next

        return function

class Group:
    def __init__(self) -> None:
        self.__name = "no name"
        self.__jobs: List[Job] = []

    @property
    def name(self) -> str:
        return self.__name
    
    @name.setter
    def name(self, n: str):
        self.__name = n

    def addJob(self, job: Job) -> None:
        self.__jobs.append(job)

    async def run(self):
        for job in self.__jobs:
            await job.run()
