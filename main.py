from workbench.jobs.console import *
from workbench.core import *
from workbench.sources import NumbersSource
from workbench.filters import *

import asyncio;

async def apply(value: Any) -> Any:
    #await asyncio.sleep(1)
    return value + 5

async def run(sheet: Worksheet):
    await sheet.run()


loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

worksheet = Worksheet(groups = [
    Group(name="Print some numbers", jobs=[
        WriteLineJob(message="Hello"),
        PipelineJob(source=NumbersSource(0, 10), filters=[
            WhereFilter(lambda x: x in [1, 3, 5]),
            SelectFilter(lambda x: "The number is " + str(x)),
            EchoFilter(),
        ]),
        WriteLineJob(message="Goodbye"),
    ]),
    Group(name="Say farewell", jobs=[
        WriteLineJob("Thats"),
        WriteLineJob("All"),
        WriteLineJob("Folks"),
    ])
])

loop.run_until_complete(run(worksheet))