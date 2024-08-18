from typing import List, AsyncIterable, Any, Optional, Callable
import abc

class WorkbenchException(Exception):
    """Base class for all workbench exceptions"""
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

class Runnable(abc.ABC): 
    """
    Base type for anything that is runnable.
    NOTE: This will get fleshed out later
    """
    pass

class Source(Runnable):
    """
    Base type for anything that provides a source of data to a filter
    """
    @abc.abstractmethod
    def run(self) -> AsyncIterable[Any]:
        pass
    

class Filter(Runnable):
    """
    Base type for a filter that takes a sequence of value, does something
    to them and yields a new sequence
    """
    @abc.abstractmethod
    def run(self, input: AsyncIterable[Any]) -> AsyncIterable[Any]:
        pass

class Job(Runnable): 
    """
    Base class for any job.
    Jobs go into a group
    """
    @abc.abstractmethod
    async def run(self) -> None:
        pass
    
class _PipelineStart:
    def __init__(self, outer: Callable[[], AsyncIterable[Any]]) -> None:
        self.__outer = outer
    
    async def __call__(self, *args: Any, **kwds: Any) -> AsyncIterable[Any]:
        outer = self.__outer

        async for i in outer():
            yield i

class _PipelineStep:
    def __init__(self, outer: Callable[[], AsyncIterable[Any]], filter: Filter) -> None:
        self.__outer = outer
        self.__filter = filter

    async def __call__(self, *args: Any, **kwds: Any) -> AsyncIterable[Any]:
        outer = self.__outer
        filter = self.__filter

        async for i in filter.run(outer()):
            yield i
    
    
class PipelineJob(Job):
    """
    A pipeline jobs takes a source of data and passes it through a series of filters
    """
    def __init__(self, source: Optional[Source] = None, filters: List[Filter] = []) -> None:
        super().__init__()
        self.__source: Optional[Source] = source
        self.__filters: List[Filter] = filters.copy()        

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
    
    def _build_pipeline(self) -> Callable[[], AsyncIterable[Any]]:
        src = self.__source
        if src is None:
            raise WorkbenchException("no source set")

        function = _PipelineStart(lambda: src.run())

        # This is a LOT easier in C# which has proper block scoping!!
        for filter in self.__filters:
            next = _PipelineStep(function, filter)
            function = next

        return function

class Group:
    """
    A group contains a sequence of jobs that are executed one after the other
    """
    def __init__(self, name: str = "no name", jobs: List[Job] = []) -> None:
        self.__name = name
        self.__jobs: List[Job] = jobs.copy()

    @property
    def name(self) -> str:
        """The name of the group"""
        return self.__name
    
    @name.setter
    def name(self, n: str):
        self.__name = n

    def addJob(self, job: Job) -> None:
        """Adds a new job to the group"""
        self.__jobs.append(job)

    async def run(self):
        print(self.__name)
        for job in self.__jobs:
            await job.run()

class Worksheet:
    """Manages a collection of groups"""
    def __init__(self, groups: List[Group] = []) -> None:
        self.__groups = groups.copy()

    async def run(self) -> None:
        """Runs each group"""
        for group in self.__groups:
            await group.run()