 arend
=============

A simple producer consumer library for Beanstalkd queue.


Basic Usage
--------------

In your code:

 ```python
from arend import arend_task


@arend_task(queue="my_queue")
def double(num: int) -> int:
    return num * 2

double.apply_async()  # create an ArendTask
```

In your worker, consume the task:
```python
from arend import consumer


consumer(queue="my_queue", polling=True)  # consume arend tasks from queue
```

In your FastAPI app:
```python
from arend import arend_router
from fastapi import FastAPI


...
app = FastAPI()
app.include_router(router=arend_router)  # expose tasks objects
...
```


Documentation
--------------

Please visit this [link](https://arend.readthedocs.io/en/latest/) for documentation.
