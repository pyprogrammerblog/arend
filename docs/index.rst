.. arend documentation master file, created by
   sphinx-quickstart on Tue Aug  9 09:38:22 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.
.. _index:

Welcome to arend' documentation!
=============================================

A simple producer-consumer library for the Beanstalkd queue!

Installation
---------------

Install it using `pip`::

   pip install arend

Basic usage
---------------

Make sure you have the `arend` installed::

   from arend import arend_task, consumer
   from arend.backends.mongo import MongoSettings
   from arend.settings import ArendSettings, BeanstalkdSettings


   settings = ArendSettings(
       broker=BeanstalkdSettings(host="beanstalkd", port=11300),
       backend=MongoSettings(
           mongo_db="db",
           mongo_collection="logs",
           mongo_connection="mongodb://user:pass@mongo:27017",
       )
       # more optional settings to be defined here...
   )

   # define your task
   @arend_task(queue="my_queue", settings=settings)
   def double(num: int) -> int:
       return num * 2

   # send it to the queue
   double.apply_async()

   # consume tasks
   consumer(queue="my_queue", settings=settings)


Backends
-------------------
The available backends to store logs are **Mongo** and **Redis**.

Setting your backend with environment variables
---------------------------------------------------
You can set your backend by defining env vars.
The `AREND__` prefix indicates that it belongs to the Arend::

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


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   arend
   settings
   mongo
   redis
   license
   help


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
