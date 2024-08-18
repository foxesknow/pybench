from workbench.jobs import console
from workbench.core import *
from workbench.sources import NumbersSource
from workbench.filters import *

import asyncio;

async def apply(value: Any) -> Any:
    await asyncio.sleep(1)
    return value + 5

async def run(group: Group):
    await group.run()


loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

group = Group(name="Test", jobs= [
    console.WriteLineJob(message="Hello"),
    PipelineJob(source=NumbersSource(0, 10), filters= [
        LambdaFilter(lambda x: x + 200),
        EchoFilter(),
        LambdaFilter(apply),
    ]),
    console.WriteLineJob(message="Goodbye"),
])

loop.run_until_complete(run(group))