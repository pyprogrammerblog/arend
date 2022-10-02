Arend
========

A simple producer-consumer library for the Beanstalkd queue.

Installation
--------------
Hit the command:
```shell
pip install arend
```

Basic Usage
--------------

In your code:
 ```python
from arend import arend_task
from arend.backends.mongo import MongoSettings
from arend.brokers import BeanstalkdSettings
from arend.settings import ArendSettings
from arend.worker import consumer

settings = ArendSettings(
    beanstalkd=BeanstalkdSettings(host="beanstalkd", port=11300),
    backend=MongoSettings(
        mongo_connection="mongodb://user:pass@mongo:27017",
        mongo_db="db",
        mongo_collection="Tasks"
    ),
)

@arend_task(queue="my_queue", settings=settings)
def double(num: int):
    return 2 * num

double(2)  # returns 4
task = double.apply_async(args=(4,))  # It is sent to the queue

consumer(queue="my_queue", settings=settings)  # consume tasks from the queue

Task = settings.get_backend()  # you can check your backend for the result
task = Task.get(uuid=task.uuid)
assert task.result == 4
```

Backends
-------------------
The available backends to store logs are **Mongo** and **Redis**.
Please read the [docs](https://arend.readthedocs.io/en/latest/) 
for further information.

Setting your backend with environment variables
--------------------------------------------------
You can set your backend by defining env vars.
The `AREND__` prefix indicates that it belongs to the Arend.
```shell
# Redis
AREND__REDIS_HOST='redis'
AREND__REDIS_DB='1'
AREND__REDIS_PASSWORD='pass'
...

# Mongo
AREND__MONGO_CONNECTION='mongodb://user:pass@mongo:27017'
AREND__MONGO_DB='db'
AREND__MONGO_COLLECTION='logs'
...
```

In your code:
 ```python
from arend import arend_task
from arend.worker import consumer


@arend_task(queue="my_queue")
def double(num: int):
    return 2 * num

double.apply_async(args=(4,))  # It is sent to the queue

consumer(queue="my_queue")
```

Documentation
--------------

Please visit this [link](https://arend.readthedocs.io/en/latest/) for documentation.
