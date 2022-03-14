from arend.worker.consumer import consumer
from multiprocessing import Pool

import click
import logging


logger = logging.getLogger(__name__)


@click.command(name="Start Processor")
@click.argument("args", nargs=-1)
def pool_processor(args):
    """
    Pool processor.

    :param args: str.

    Example:
    ```python3 /path/to/pool_processor --queue_1=2 --queue_2=3```
    """

    # parse arguments
    queues = []
    for arg in args:
        queue, concurrency = arg.split("=")
        queues.extend([queue] * int(concurrency))

    # get the pool and map the consumers
    with Pool(processes=len(queues)) as pool:
        pool.map(consumer, queues)
