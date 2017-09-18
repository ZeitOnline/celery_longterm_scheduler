import collections
import json
import pickle


class MemoryBackend(object):

    def __init__(self, unused_url):
        self.by_id = {}
        self.by_time = collections.defaultdict(list)

    def set(self, timestamp, task_id, args, kw):
        if timestamp.tzinfo is None:
            raise ValueError('Timezone required, got %s', timestamp)
        self.by_id[task_id] = json.dumps([args, kw])
        self.by_time[timestamp].append(task_id)

    def get(self, task_id):
        args, kw = json.loads(self.by_id[task_id])
        if isinstance(kw['args'], list):
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
        for ts in sorted(self.by_time.keys()):
            if ts > timestamp:
                break
            for id in self.by_time[ts]:
                yield (id, self.get(id))
