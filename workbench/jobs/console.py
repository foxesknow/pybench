from .. import core

import sys
import asyncio

class NullJob(core.Job):
    async def run(self):
        # Does nothing
        pass

class EchoStdoutJob(core.Job):
    def __init__(self, message: str = "no message") -> None:
        super().__init__()
        self.__message = message

    async def run(self) -> None:
        print(self.__message) 
    
    @property
    def message(self):
        return self.__message

    @message.setter
    def message(self, text: str):
        self.__message = text


class EchoStderrJob(core.Job):
    def __init__(self, message: str = "no message") -> None:
        super().__init__()
        self.__message = message

    async def run(self) -> None:
        print(self.__message, file=sys.stderr) 
    
    @property
    def message(self):
        return self.__message

    @message.setter
    def message(self, text: str):
        self.__message = text


class SleepJob(core.Job):
    def __init__(self, seconds: float = 0) -> None:
        super().__init__()
        self.__seconds = seconds

    async def run(self) -> None:
        await asyncio.sleep(self.__seconds)
    
    @property
    def seconds(self) -> float :
        return self.__seconds

    @seconds.setter
    def seconds(self, s: float):
        self.__seconds = s