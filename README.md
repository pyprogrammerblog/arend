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

double.apply_async()  # create a task and send it to the queue
```

In your worker, consume the task:
```python
from arend.consumer import consumer

consumer(queue="my_queue", long_polling=True)  # consume tasks from queue
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
