=========================
celery_longterm_scheduler
=========================

Schedules celery tasks to run in the potentially far future, using a separate
storage backend (currently only redis is supported) in combination with a
cronjob.


Usage
=====

* Configure the storage by adding a setting like ``longterm_scheduler_backend =
  'redis://localhost:6739/1'`` to your celery configuration.
  (The storage also respects the built-in celery configuration settings
  ``redis_socket_timeout``, ``redis_socket_connect_timeout`` and
  ``redis_max_connections``.)
* Configure your celery app to use a customized task class
  ``MYCELERY = celery.Celery(task_cls=celery_longterm_scheduler.Task)``
* Set up a cronjob to run ``celery longterm_scheduler`` (e.g. every 5 minutes)
* Now you can schedule your tasks by calling
  ``mytask.apply_async(args, kwargs, eta=datetime)`` as normal. This returns
  a normal ``AsyncResult`` object, but only reading the ``.id`` is supported;
  any other methods or properties may fail explictly or implicitly.
* You can completely delete a scheduled job by calling
  ``celery_longterm_scheduler.get_scheduler(MYCELERY).revoke('mytaskid')``
  (we cannot hook into the celery built-in ``AsyncResult.revoke()``,
  unfortunately). ``revoke()`` returns True on success and False if the given
  task cannot be found in the storage backend (e.g. because it has already come
  due and been executed).

Instead of sending a normal job to the celery broker (with added timing
information), this creates a job entry in the scheduler storage backend. The
cronjob then periodically checks the storage for any jobs that are due, and
only then sends a normal celery job to the broker.


Rationale
=========

Why not use the celery built-in ``apply_async(eta=)``? Because you cannot ever
really delete a pending job. ``AsyncResult('mytaskid').revoke()`` can only add
the task ID to the statedb, where it has to stay _forever_ so the job is
recognized as revoked. For jobs that are scheduled to run in 6 months time or
later, this would create an unmanageable, ever-growing statedb.

Why not use celerybeat? Because it is built for periodic jobs, and we need
single-shot jobs. And then there's not much to gain from the celerybeat
implementation, especially since we want to use redis as storage (since we're
already using that as broker and result backend).


Implementation
==============

Redis schema
------------

celery_longterm_scheduler assumes that it talks to a dedicated redis database.
It creates an entry per scheduled job using ``SET jobid job-configuration``
(job-configuration is serialized with JSON) and uses a single sorted set named
``scheduled_task_id_by_time`` that contains the jobids scored by the unix
timestamp (UTC) when they are due.


Run tests
=========

Using `tox`_ and `py.test`_. Maybe install ``tox`` (e.g. via ``pip install tox``)
and then simply run ``tox``.

For the integration tests you need to have the redis binary installed (tests
start `their own server`_).

.. _`tox`: http://tox.readthedocs.io/
.. _`py.test`: http://pytest.org/
.. _`their own server`: https://pypi.python.org/pypi/testing.redis
