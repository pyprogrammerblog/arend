from arend.settings import settings
from arend.worker.consumer import consumer
from multiprocessing import Pool

import logging


logger = logging.getLogger(__name__)


__all__ = ["pool_processor"]


def pool_processor():
    """
    Pool processor.

    Example:

    ```/path/to/python3 /path/to/processor.py```
    """

    if not settings.queues:
        raise ValueError("Please define queue(s) in settings")

    queues = []
    for queue, concurrency in settings.queues.items():
        queues.extend([queue] * int(concurrency))

    # get the pool and map the consumers
    with Pool(processes=len(queues)) as pool:
        pool.map(consumer, queues)


if __name__ == "__main__":
    pool_processor()
