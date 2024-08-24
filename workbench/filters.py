from typing import AsyncIterable, Any, List, Callable
import asyncio

from workbench.core import Filter

class EchoFilter(Filter):
    """Echos each item to stdout"""
    async def run(self, input: AsyncIterable[Any]) -> AsyncIterable[Any]:
        async for value in input:
            print(value)
            yield value

class SleepFilter(Filter):
    """Sleeps for a period of time"""
    def __init__(self, seconds: float = 0) -> None:
        super().__init__()
        self.__sleep = seconds

    async def run(self, input: AsyncIterable[Any]) -> AsyncIterable[Any]:
        async for value in input:
            await asyncio.sleep(self.__sleep)
            yield value
    

class ReverseFilter(Filter):
    """
    Reverse the entire interable input.
    NOTE: This requires it to consume the entire input first
    """
    async def run(self, input: AsyncIterable[Any]) -> AsyncIterable[Any]:
        incoming_data : List[Any] = [] 
        
        async for value in input:
            incoming_data.append(value)

        for i in reversed(incoming_data):
            yield i


class SelectFilter(Filter):
    """
    A filter that calls a lambda for each item to transform the item.
    The filter can handle synchronous and asynchronous functions.
    """
    def __init__(self, function: Callable[[Any], Any]) -> None:
        super().__init__()

        self.function = function

        if asyncio.iscoroutinefunction(function):
            self.__runAsync = True
        elif callable(function):
            self.__runAsync = False
        else:
            raise ValueError("function is not callable")
    
    def run(self, input: AsyncIterable[Any]) -> AsyncIterable[Any]:
        if self.__runAsync:
            return self._run_async(input)
        else:
            return self._run_sync(input)

    async def _run_async(self, input: AsyncIterable[Any]) -> AsyncIterable[Any]:
        function = self.function
        
        async for value in input:
            new_value = await function(value)
            yield new_value

    async def _run_sync(self, input: AsyncIterable[Any]) -> AsyncIterable[Any]:
        function = self.function
        
        async for value in input:
            new_value = function(value)
            yield new_value


class PredicateFilterBase(Filter):
    """
    Base class for filters that use a predicate to do something
    """
    def __init__(self, predicate: Callable[[Any], Any]) -> None:
        super().__init__()

        self.predicate = predicate

        if asyncio.iscoroutinefunction(predicate):
            self.__runAsync = True
        elif callable(predicate):
            self.__runAsync = False
        else:
            raise ValueError("function is not callable")
    
    async def _apply(self, value: Any):
        predicate = self.predicate

        if self.__runAsync:
            return await predicate(value)
        else:
            return predicate(value)    
        
class TakeWhileFilter(PredicateFilterBase):
    """
    Yields values until the predicate returns false, at which point we'll stop processing
    """
    def __init__(self, predicate: Callable[[Any], Any]) -> None:
        super().__init__(predicate)

    async def run(self, input: AsyncIterable[Any]) -> AsyncIterable[Any]:
        async for value in input:
            if await self._apply(value):
                yield value
            else:
                break
            
class TakeUntilFilter(PredicateFilterBase):
    """
    Yields values until the predicate returns false, at which point we'll stop processing
    """
    def __init__(self, predicate: Callable[[Any], Any]) -> None:
        super().__init__(predicate)

    async def run(self, input: AsyncIterable[Any]) -> AsyncIterable[Any]:
        async for value in input:
            if await self._apply(value):
                break
            else:
                yield value


class WhereFilter(PredicateFilterBase):
    """
    Yields values where the predicates returns true
    """
    def __init__(self, predicate: Callable[[Any], Any]) -> None:
        super().__init__(predicate)

    async def run(self, input: AsyncIterable[Any]) -> AsyncIterable[Any]:
        async for value in input:
            if await self._apply(value):
                yield value