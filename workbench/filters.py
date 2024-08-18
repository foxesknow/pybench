from typing import AsyncIterable, Any, List
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
