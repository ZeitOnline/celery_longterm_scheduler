import collections
import json
import pendulum
import pickle


class MemoryBackend(object):

    def __init__(self, unused_url):
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


def serialize_timestamp(timestamp):
    """Converts a datetime into seconds since the epoch."""
    return int(pendulum.instance(timestamp).timestamp())


# Could be made extensible via entrypoints, like in celery.app.backends.
BACKENDS = {
    'memory': MemoryBackend,
}


def by_url(url):
    if '://' not in url:
        raise ValueError(
            'longterm_scheduler_backend must be an URL, got %r' % url)
    scheme = url.split('://')[0]
    return BACKENDS[scheme](url)


class PickleFallbackJSONEncoder(json.JSONEncoder):

    PICKLE_MARKER = '__python_pickle__'

    def default(self, o):
        raw = pickle.dumps(o)
        return self.PICKLE_MARKER + raw

    @classmethod
    def decode_dict(cls, o):
        for key, value in o.items():
            if isinstance(value, basestring) and value.startswith(
                    cls.PICKLE_MARKER):
                raw = value.replace(cls.PICKLE_MARKER, '', 1)
                o[key] = pickle.loads(raw)
        return o


def serialize(obj):
    return json.dumps(obj, cls=PickleFallbackJSONEncoder)


def deserialize(string):
    return json.loads(
        string, object_hook=PickleFallbackJSONEncoder.decode_dict)
