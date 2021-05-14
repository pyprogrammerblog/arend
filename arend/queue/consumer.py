from pystalkd.Beanstalkd import Connection
from arend.queue.task import QueueTask
from arend.settings import base

import sys


def consumer(queue_name: str, verbose: bool = False):

    beanstalk = Connection(
        host=base.BEANSTALKD_HOST, port=base.BEANSTALKD_PORT
    )
    beanstalk.watch(queue_name)

    while True:
        sys.stdout.write(f"Queue: '{queue_name}'. Waiting for messages...")
        message = beanstalk.reserve()

        sys.stdout.write(f"Message with id: {message.body} reserved...")
        task = QueueTask.get(id=message.body)

        sys.stdout.write("Executing task...")
        task.run()  # run sync inside worker

        sys.stdout.write("Deleting message from queue...")
        message.delete()
