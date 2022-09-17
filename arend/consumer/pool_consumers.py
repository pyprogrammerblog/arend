from arend.consumer.consumer import consumer
from multiprocessing import Pool

import click
import logging


__all__ = ["pool_consumers"]


logger = logging.getLogger(__name__)


@click.command(name="Start Pool of Consumers")
@click.argument("args", nargs=-1)
def pool_consumers(args):
    """
    Pool Consumers.

    :param args: str.

    Example:
    ```python3 pool_processor --queue_1=4 --queue_2=2```
    """

    # parse arguments
    queues = []
    for arg in args:
        queue, concurrency = arg.split("=")
        queues.extend([queue] * int(concurrency))

    # get the pool and map the consumers
    with Pool(processes=len(queues)) as pool:
        pool.map(consumer, queues)
