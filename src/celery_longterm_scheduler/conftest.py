import celery
import celery.contrib.testing.app
import celery_longterm_scheduler
import pytest
import redis
import testing.redis


CELERY = celery.Celery(task_cls=celery_longterm_scheduler.Task)


@pytest.fixture(scope='session')
def celery_worker(request):
    CELERY.conf.update(celery.contrib.testing.app.DEFAULT_TEST_CONFIG)
    CELERY.conf['longterm_scheduler_backend'] = 'memory://'
    worker = celery.contrib.testing.worker.start_worker(CELERY)
    worker.__enter__()
    request.addfinalizer(lambda: worker.__exit__(None, None, None))


# celery.contrib.testing.worker expects a 'ping' task, so it can check that the
# worker is running properly.
@CELERY.task(name='celery.ping')
def celery_ping():
    return 'pong'


@pytest.fixture(scope='session')
def redis_server_session():
    server = testing.redis.RedisServer()
    yield server
    server.stop()


@pytest.fixture
def redis_server(redis_server_session):
    yield redis_server_session
    client = redis.StrictRedis(**redis_server_session.dsn())
    for key in client.keys():
        client.delete(key)
