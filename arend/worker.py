from arend.tube.consumer import consumer
from multiprocessing import Pool

import click
import logging


logger = logging.getLogger(__name__)


@click.command(name="Start Processor")
@click.argument("args", nargs=-1)
def pool_processor(args):

    # parse arguments
    queues = []
    for arg in args:
        queue, concurrency = arg.split("=")
        queues.extend([queue] * int(concurrency))

    # get the pool and map the consumers
    with Pool(processes=len(queues)) as pool:
        pool.map(consumer, queues)


if __name__ == "__main__":
    pool_processor()
