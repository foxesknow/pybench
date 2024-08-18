from typing import AsyncIterator, Any, List
import asyncio

from workbench.core import Filter

class EchoFilter(Filter):
    async def run(self, input: AsyncIterator[Any]) -> AsyncIterator[Any]:
        async for value in input:
            print(value)
            yield value

class SleepFilter(Filter):
    def __init__(self, seconds: float = 0) -> None:
        super().__init__()
        self.__sleep = seconds

    async def run(self, input: AsyncIterator[Any]) -> AsyncIterator[Any]:
        async for value in input:
            await asyncio.sleep(self.__sleep)
            yield value
    
            
class ReverseFilter(Filter):
    async def run(self, input: AsyncIterator[Any]) -> AsyncIterator[Any]:
        incoming_data : List[Any] = [] 
        
        async for value in input:
            incoming_data.append(value)

        for i in reversed(incoming_data):
            yield i
