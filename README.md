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
from arend import arend_task, consumer
from arend.backends.mongo import MongoSettings
from arend.settings import ArendSettings, BeanstalkdSettings


settings = ArendSettings(
    task_delay=1,
    task_delay_factor=1,
    broker=BeanstalkdSettings(
        host="beanstalkd", port=11300
    ),
    backend=MongoSettings(
        mongo_db="db",
        mongo_collection="logs",
        mongo_connection="mongodb://user:pass@mongo:27017",
    ),
)

@arend_task(queue="my_queue", settings=settings)
def double(num: int) -> int:
    return num * 2

# create a task and send it to the queue
double.apply_async()

# consume tasks from queue
consumer(queue="my_queue", settings=settings)
```

Backends
-------------------
The available backends to store logs are **Mongo**, **Redis** and **SQL**.
Please read the [docs](https://arend.readthedocs.io/en/latest/) 
for further information.

Setting your backend with environment variables
--------------------------------------------------
You can set your backend by defining env vars.
The `AREND__` prefix indicates that it belongs to `ProgressUpdater`.
```shell
# SQL
AREND__SQL_DSN='postgresql+psycopg2://user:pass@postgres:5432/db'
AREND__SQL_TABLE='logs'
...

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

Documentation
--------------

Please visit this [link](https://arend.readthedocs.io/en/latest/) for documentation.
