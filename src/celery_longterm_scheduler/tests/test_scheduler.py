from celery_longterm_scheduler.conftest import CELERY
import celery_longterm_scheduler
import mock
import pendulum
import time


ANYTIME = pendulum.create(2017, 1, 20)
record_calls = []


@CELERY.task
def record(arg):
    record_calls.append(arg)


def test_execute_pending_runs_scheduled_tasks(celery_app):
    record_calls[:] == []
    due = ANYTIME
    record.apply_async(('foo',), eta=due)
    assert not record_calls
    scheduler = celery_longterm_scheduler.get_scheduler(CELERY)
    scheduler.execute_pending(due)
    # XXX I don't think there is any "job completed" signal we could wait for.
    time.sleep(1)
    assert record_calls == ['foo']


@CELERY.task
def echo(arg):
    return arg


def test_execute_pending_deletes_from_storage():
    due = ANYTIME
    echo.apply_async(('foo',), eta=due)

    scheduler = celery_longterm_scheduler.get_scheduler(CELERY)
    pending = list(scheduler.backend.get_older_than(due))
    assert pending

    with mock.patch.object(CELERY, 'send_task'):
        scheduler.execute_pending(due)
    pending = list(scheduler.backend.get_older_than(due))
    assert not pending


def test_revoke_deletes_from_storage():
    due = ANYTIME
    id = echo.apply_async(('foo',), eta=due).id
    scheduler = celery_longterm_scheduler.get_scheduler(CELERY)
    pending = list(scheduler.backend.get_older_than(due))
    assert pending

    scheduler.revoke(id)
    pending = list(scheduler.backend.get_older_than(due))
    assert not pending


def test_revoke_returns_false_for_nonexistent_id():
    scheduler = celery_longterm_scheduler.get_scheduler(CELERY)
    assert scheduler.revoke('nonexistent') is False
