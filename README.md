 arend
=============

A simple producer consumer library for Beanstalkd queue.


Basic Usage
--------------

In your code:
```python
from arend import arend_task

@arend_task(queue_name="my_queue")
def double(num: int) -> int:
    return num * 2

double.apply_async()  # create an ArendTask
```

In your worker:
```python
from arend.worker import consumer

consumer(queue_name="my_queue", polling=True)  # consume tasks form queue
```

In your FastAPI app:
```python
from arend.api import arend_router
from fastapi import FastAPI


app = FastAPI()
app.include_router(router=arend_router)  # expose tasks objects
...
```


Documentation
--------------

Please visit this [link](https://arend.readthedocs.io/en/latest/) for documentation.
