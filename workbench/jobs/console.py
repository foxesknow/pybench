from .. import core

import sys
import asyncio

class NullJob(core.Job):
    async def run(self):
        # Does nothing
        pass

class WriteLineJob(core.Job):
    """Echos a message to stdout"""
    def __init__(self, message: str = "no message", stdout: bool = True) -> None:
        super().__init__()
        self.__message = message
        self.__stdout = stdout

    async def run(self) -> None:
        if self.__stdout:
            print(self.__message) 
        else:
            print(self.__message, file=sys.stderr) 
    
    @property
    def message(self):
        return self.__message

    @message.setter
    def message(self, text: str):
        self.__message = text


class SleepJob(core.Job):
    """Sleeps for a number of seconds"""
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