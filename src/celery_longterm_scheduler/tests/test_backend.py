"""Contract test that every scheduler storage backend must comply with."""
from datetime import datetime
import celery_longterm_scheduler.backend
import pytest
import pytz


ANYTIME = datetime(2017, 1, 20, tzinfo=pytz.UTC)


@pytest.fixture(params=['memory://'])
def backend(request, redis_server):
    url = request.param
    return celery_longterm_scheduler.backend.by_url(url)


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


def test_get_nonexistent_task_id_raises_keyerror(backend):
    with pytest.raises(KeyError):
        backend.get('nonexistent')


def test_delete_task_can_not_be_retrieved_with_get(backend):
    backend.set(ANYTIME, 'one', ('arg1',), {'kw1': None})
    backend.set(ANYTIME, 'two', ('arg2',), {'kw2': None})
    backend.delete('one')
    with pytest.raises(KeyError):
        backend.get('one')
    assert backend.get('two') == (('arg2',), {'kw2': None})


def test_get_older_than_returns_timestamps_smaller_or_equal(backend):
    backend.set(datetime(2017, 1, 1, 9, tzinfo=pytz.UTC), '1', (1,), {'1': 1})
    backend.set(datetime(2017, 1, 1, 10, tzinfo=pytz.UTC), '2', (2,), {'2': 2})
    backend.set(datetime(2017, 1, 1, 11, tzinfo=pytz.UTC), '3', (3,), {'3': 3})
    items = list(
        backend.get_older_than(datetime(2017, 1, 1, 10, tzinfo=pytz.UTC)))
    assert len(items) == 2
    assert items[0][0] == '1'
    assert items[0][1] == ((1,), {'1': 1})
    assert items[1][0] == '2'
    assert items[1][1] == ((2,), {'2': 2})
