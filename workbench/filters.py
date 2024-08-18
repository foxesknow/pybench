from typing import AsyncIterable, Any, List, Callable, Awaitable
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

class LambdaFilter(Filter):
    """
    A filter that calls a lambda for each item
    """
    def __init__(self, function: Callable[[Any], Any]) -> None:
        super().__init__()

        if asyncio.iscoroutinefunction(function):
            raise ValueError("async callable not supported - you should use AsyncLambdaFilter")

        if not callable(function):
            raise ValueError("function is not callable")

        self.function = function

    async def run(self, input: AsyncIterable[Any]) -> AsyncIterable[Any]:
        function = self.function
        
        async for value in input:
            new_value = function(value)
            yield new_value

class AsyncLambdaFilter(Filter):
    """
    A filter that asynchronously calls a lambda for each item
    """
    def __init__(self, function: Callable[[Any], Awaitable[Any]]) -> None:
        super().__init__()

        if not asyncio.iscoroutinefunction(function):
            raise ValueError("function is not async callable")

        self.function = function

    async def run(self, input: AsyncIterable[Any]) -> AsyncIterable[Any]:
        function = self.function
        
        async for value in input:
            new_value = await function(value)
            yield new_value