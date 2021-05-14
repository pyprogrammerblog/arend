from arend.queue.consumer import consumer
from multiprocessing import Pool
from typing import List

import click
import logging

logger = logging.getLogger(__name__)


@click.command(name="Start Processor")
@click.option('--queues', type=List[str], help='Queue names')
@click.option('--concurrency', type=int, default=1, help='Queue concurrency')
@click.option('--verbose', is_flag=True, default=False, help='Verbose')
def pool_processor(queues, concurrency, verbose):
    click.echo("Getting the Pool...")
    p = Pool()
    try:
        click.echo("Starting workers...")
        p.map(consumer, [(queue, verbose) for queue in queues] * concurrency)
        p.close()
    except KeyboardInterrupt:
        click.echo("Keyboard Exception, terminating pool...")
        p.terminate()
        click.echo("Pool terminated...")
    except Exception as e:
        click.echo(f"Exception '{e}', terminating pool...")
        p.terminate()
        click.echo("Pool terminated...")
    finally:
        click.echo("Joining processes...")
        p.join()
        click.echo("Processes joined...")


if __name__ == "__main__":
    pool_processor()
