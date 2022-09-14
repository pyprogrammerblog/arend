 arend
=============

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

@arend_task(queue="my_queue")
def double(num: int) -> int:
    return num * 2

double.apply_async()  # create a task on your Backend and send it to the queue
```

In your worker, consume the task:
```python
from arend import consumer

consumer(queue="my_queue", long_polling=True)  # consume tasks from queue
```

Backends
----------
There are three backends available to save our tasks.

1. Mongo.
2. Redis.
3. SQL.

There are some possible ways to pass backend settings. This is the priority.

1. **Passing settings as parameters**.

```python
from arend import arend_task
from arend.backends import MongoSettings

settings = MongoSettings(
    mongo_connection="mongodb://user:pass@mongo:27017",
    mongo_db="db",
    mongo_collection="logs",
)

@arend_task(queue="my_queue", settings=settings)
def double(num: int) -> int:
    return num * 2
```

```python
from arend import consumer

consumer(queue="my_queue", long_polling=True, settings=settings)
```

2. **Environment variables**.

The `AREND__` prefix indicates that it belongs to Arend.
```shell
export AREND__SQL_DSN=postgresql+psycopg2://user:pass@postgres:5432/db
export AREND__SQL_TABLE=logs
```

Documentation
--------------

Please visit this [link](https://arend.readthedocs.io/en/latest/) for documentation.
