from workbench.jobs import console
from workbench.core import *
from workbench.sources import NumbersSource
from workbench.filters import *

import asyncio;

async def apply(value: Any) -> Any:
    await asyncio.sleep(0.5)
    return value + 5

async def run(group: Group):
    await group.run()


echo1 = console.EchoStdoutJob(message="Hello");
sleep = console.SleepJob(seconds=2);
echo2 = console.EchoStdoutJob(message="world");

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

group = Group()
group.addJob(console.EchoStdoutJob(message="Hello"))



pipeline = PipelineJob();
pipeline.source = NumbersSource(0, 10)
pipeline.add_filter(AsyncLambdaFilter(apply))
pipeline.add_filter(EchoFilter())
#pipeline.add_filter(SleepFilter(seconds=1))
#pipeline.add_filter(ReverseFilter())
#pipeline.add_filter(EchoFilter())
group.addJob(pipeline)
group.addJob(console.EchoStdoutJob(message="Goodbye"))

loop.run_until_complete(run(group))