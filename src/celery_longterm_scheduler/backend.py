import base64
import celery.backends.redis
import collections
import json
import pendulum
import pickle
import redis
import sys


if sys.version_info < (3,):
    text_type = unicode
else:
    text_type = str


class AbstractBackend(object):
    """Interface for the scheduler storage backend. Also see test_backend.py
    for the corresponding contract tests that every implementation must pass.

    Clients should not instantiate a backend class themselves, but rather
    use the ``by_url()`` mechanism.
    """

    def __init__(self, url, app):
        """
        :param url: string, a backend-specific configuration address
        :param app: celery App instance (mainly so the backend can access the
        configuration in ``app.conf``)
        """
        raise NotImplementedError()

    def set(self, timestamp, task_id, args, kw):
        """Stores ``args`` and ``kw`` under ``task_id`` and ``timestamp``.
        args and kw are serialized using JSON.

        :param timestamp: timezone-aware datetime
        :param task_id: string
        :param args: tuple, positional arguments for the task
        :param kw: dict, keyword arguments for the task
        """
        raise NotImplementedError()

    def get(self, task_id):
        """Retrieves a task entry stored by ``set()``

        :param task_id: string
        :returns: tuple (args, kw) -- args tuple, kw dict
        """
        raise NotImplementedError()

    def delete(self, task_id):
        """Removes a task entry stored by ``set()``.

        :raises: KeyError if ``task_id`` was not found
        """
        raise NotImplementedError()

    def get_older_than(self, timestamp):
        """Retrieves task entries scheduled for times older or equal than
        ``timestamp``.

        :param timestamp: timezone-aware datetime
        :return: iterable of tuple (task_id, (args, kw))
        """
        raise NotImplementedError()


class MemoryBackend(AbstractBackend):
    """In-memory backend implementation, for tests."""

    def __init__(self, unused_url, unused_app):
        self.by_id = {}
        self.by_time = collections.defaultdict(list)

    def set(self, timestamp, task_id, args, kw):
        if timestamp.tzinfo is None:
            raise ValueError('Timezone required, got %s', timestamp)
        self.by_id[task_id] = serialize([args, kw])
        self.by_time[serialize_timestamp(timestamp)].append(task_id)

    def get(self, task_id):
        args, kw = deserialize(self.by_id[task_id])
        if isinstance(kw.get('args'), list):
            kw['args'] = tuple(kw['args'])
        return (tuple(args), kw)

    def delete(self, task_id):
        del self.by_id[task_id]
        for ts, id in self.by_time.items():
            if id == task_id:
                break
        bucket = self.by_time[ts]
        bucket.remove(task_id)
        if not bucket:
            del self.by_time[ts]

    def get_older_than(self, timestamp):
        timestamp = serialize_timestamp(timestamp)
        for ts in sorted(self.by_time.keys()):
            if ts > timestamp:
                break
            for id in self.by_time[ts]:
                yield (id, self.get(id))


class RedisBackend(AbstractBackend):
    """Default backend implementation: redis"""

    redis = redis
    # This is persisted in redis, only change when also having a migration plan
    BY_TIME_KEY = 'scheduled_task_id_by_time'

    def __init__(self, url, app):
        self.url = url
        self.app = app
        # Taken from celery.backends.redis.RedisBackend.__init__()
        max_connections = app.conf.get('redis_max_connections')
        socket_timeout = app.conf.get('redis_socket_timeout')
        socket_connect_timeout = app.conf.get('redis_socket_connect_timeout')
        self.connparams = {
            'max_connections': max_connections,
            'socket_timeout': socket_timeout and float(socket_timeout),
            'socket_connect_timeout':
                socket_connect_timeout and float(socket_connect_timeout),
        }
        # Sneaky "inherit this one method only" transplant
        self._params_from_url = celery.backends.redis.RedisBackend.\
            _params_from_url.__get__(self)
        self.connparams = self._params_from_url(url, self.connparams)
        # We probably don't need a parameterizeable ConnectionPool, like
        # celery.backends.redis.RedisBackend._create_client() does.
        self.client = self.redis.StrictRedis(
            connection_pool=self.redis.ConnectionPool(**self.connparams))

    def set(self, timestamp, task_id, args, kw):
        if timestamp.tzinfo is None:
            raise ValueError('Timezone required, got %s', timestamp)
        timestamp = serialize_timestamp(timestamp)
        self.client.set(task_id, serialize([args, kw]))
        self.client.zadd(self.BY_TIME_KEY, mapping={task_id: timestamp})

    def get(self, task_id):
        task = self.client.get(task_id)
        if task is None:
            raise KeyError(task_id)
        args, kw = deserialize(task)
        if isinstance(kw.get('args'), list):
            kw['args'] = tuple(kw['args'])
        return (tuple(args), kw)

    def delete(self, task_id):
        removed = 0
        removed += self.client.delete(task_id)
        removed += self.client.zrem(self.BY_TIME_KEY, task_id)
        if removed != 2:
            raise KeyError(task_id)

    def get_older_than(self, timestamp):
        timestamp = serialize_timestamp(timestamp)
        for id in self.client.zrangebyscore(self.BY_TIME_KEY, 0, timestamp):
            # Typically celery uses uuid, so ascii would suffice, but who knows
            # what kind of ids random applications use in the wild.
            yield (id.decode('utf-8'), self.get(id))


# Could be made extensible via entrypoints, like in celery.app.backends.
BACKENDS = {
    'memory': MemoryBackend,
    'redis': RedisBackend,
}


def by_url(url, app):
    if '://' not in url:
        raise ValueError(
            'longterm_scheduler_backend must be an URL, got %r' % url)
    scheme = url.split('://')[0]
    return BACKENDS[scheme](url, app)


class PickleFallbackJSONEncoder(json.JSONEncoder):
    """Serializes non-native JSON types using pickle.

    We need this mostly because Task.apply_async() needs to store itself (the
    Task instance), since send_task() needs it e.g. for routing information.
    So we *hope* that nobody puts anything non-pickleable onto their tasks.
    """

    PICKLE_MARKER = '__python_pickle__'

    def default(self, o):
        raw = pickle.dumps(o)
        if sys.version_info >= (3,):
            # The py2 pickle protocol is ascii compatible, but py3 is binary.
            raw = base64.b64encode(raw).decode('ascii')
        return self.PICKLE_MARKER + raw

    @classmethod
    def decode_dict(cls, o):
        for key, value in o.items():
            if isinstance(value, text_type) and value.startswith(
                    cls.PICKLE_MARKER):
                raw = value.replace(cls.PICKLE_MARKER, '', 1)
                if sys.version_info >= (3,):
                    import binascii
                    try:
                        raw = base64.b64decode(raw)
                    except binascii.Error:
                        # We hopefully have a py2 pickle.
                        raw = raw.encode('ascii')
                o[key] = pickle.loads(raw)
        return o


def serialize(obj):
    return json.dumps(obj, cls=PickleFallbackJSONEncoder)


def deserialize(string):
    return json.loads(
        string, object_hook=PickleFallbackJSONEncoder.decode_dict)


def serialize_timestamp(timestamp):
    """Converts a datetime into seconds since the epoch."""
    return int(pendulum.instance(timestamp).timestamp())
