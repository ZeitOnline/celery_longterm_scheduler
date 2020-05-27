"""Contract test that every scheduler storage backend must comply with."""
from datetime import datetime
import celery
import celery_longterm_scheduler.backend
import celery_longterm_scheduler.conftest
import json
import pendulum
import pytest


ANYTIME = pendulum.create(2017, 1, 20)


@pytest.fixture(params=['memory://', 'redis://'])
def backend(request, redis_server):
    url = request.param
    if url == 'redis://':
        url += '{host}:{port}/{db}'.format(**redis_server.dsn())
    dummyapp = celery.Celery()
    return celery_longterm_scheduler.backend.by_url(url, dummyapp)


def test_task_passed_to_set_can_be_retrieved_with_get(backend):
    args = ('arg',)
    kw = {'kw': None}
    backend.set(ANYTIME, 'myid', args, kw)
    task = backend.get('myid')
    assert task[0] == args
    assert task[1] == kw


def test_set_requires_timezone_aware_datetime(backend):
    with pytest.raises(ValueError):
        backend.set(datetime.now(), 'myid', (), {})


def test_set_works_with_datetime(backend):
    due = datetime.now(pendulum.timezone('UTC'))
    backend.set(due, 'myid', (), {})
    tasks = list(backend.get_older_than(due))
    assert len(tasks) == 1
    assert tasks[0][0] == 'myid'


def test_get_nonexistent_task_id_raises_keyerror(backend):
    with pytest.raises(KeyError):
        backend.get('nonexistent')


def test_positional_args_and_the_kw_item_called_args_use_type_tuple(backend):
    # Even though we use JSON for serialization, which uses lists instead of
    # tuples, the Python convention is that positional arguments are a tuple.
    backend.set(ANYTIME, 'myid', ('arg',), {'args': ('celery-kw-arg',)})
    task = backend.get('myid')
    assert isinstance(task[0], tuple)
    assert isinstance(task[1]['args'], tuple)


def test_delete_task_can_not_be_retrieved_with_get_or_older_than(backend):
    backend.set(ANYTIME, 'one', ('arg1',), {'kw1': None})
    backend.set(ANYTIME, 'two', ('arg2',), {'kw2': None})
    backend.delete('one')
    with pytest.raises(KeyError):
        backend.get('one')
    assert backend.get('two') == (('arg2',), {'kw2': None})
    assert list(backend.get_older_than(ANYTIME)) == [
        ('two', (('arg2',), {'kw2': None}))]


def test_delete_nonexistend_task_id_raises_keyerror(backend):
    with pytest.raises(KeyError):
        backend.delete('nonexistent')


def test_get_older_than_returns_timestamps_smaller_or_equal(backend):
    backend.set(
        pendulum.create(2017, 1, 1, 9), '1', (1,), {'1': 1})
    backend.set(
        pendulum.create(2017, 1, 1, 10), '2', (2,), {'2': 2})
    backend.set(
        pendulum.create(2017, 1, 1, 11), '3', (3,), {'3': 3})
    items = list(
        backend.get_older_than(pendulum.create(2017, 1, 1, 10)))
    assert len(items) == 2
    assert items[0][0] == '1'
    assert items[0][1] == ((1,), {'1': 1})
    assert items[1][0] == '2'
    assert items[1][1] == ((2,), {'2': 2})


def test_py3_deserializes_py2_pickle():
    from celery_longterm_scheduler.backend import PickleFallbackJSONEncoder
    marker = PickleFallbackJSONEncoder.PICKLE_MARKER
    py2_pickle = ("ccelery.app.registry\n_unpickle_task_v2\np0\n"
                  "(S'celery.ping'\np1\nS'celery_longterm_scheduler.conftest'"
                  "\np2\ntp3\nRp4\n.")
    loaded = celery_longterm_scheduler.backend.deserialize(
        json.dumps({'task_type': marker + py2_pickle}))['task_type']
    ping = celery_longterm_scheduler.conftest.celery_ping.__maybe_evaluate__()
    # Sigh, this proxy business prevents doing a simple object identity check.
    assert repr(type(loaded)) == repr(type(ping))
