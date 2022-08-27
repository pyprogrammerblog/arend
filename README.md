 arend
=============

A simple producer-consumer library for Beanstalkd queue.

The arend uses Mongo as Backend and Beanstalkd as queue. 
It also brings a FastApi Router to access via REST your tasks.

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

consumer(queue="my_queue", polling=True)  # consume tasks from queue
```

In your FastAPI app:
```python
from arend import arend_router
from fastapi import FastAPI

app = FastAPI()
app.include_router(router=arend_router)  # expose tasks objects
...
```

Documentation
--------------

Please visit this [link](https://arend.readthedocs.io/en/latest/) for documentation.
