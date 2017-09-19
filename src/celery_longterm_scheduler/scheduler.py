from celery_longterm_scheduler import backend
import logging


log = logging.getLogger(__name__)


class Scheduler(object):

    CONF_KEY = '__longterm_scheduler_backend'

    def __init__(self, app):
        self.app = app
        if self.CONF_KEY not in app.conf:
            app.conf[self.CONF_KEY] = backend.by_url(
                app.conf['longterm_scheduler_backend'])
        self.backend = app.conf[self.CONF_KEY]

    @classmethod
    def from_app(cls, app):
        return cls(app)

    def store(self, timestamp, task_id, args, kw):
        self.backend.set(timestamp, task_id, args, kw)

    def execute_pending(self, timestamp):
        log.info('Start executing tasks older than %s', timestamp)
        for id, task in self.backend.get_older_than(timestamp):
            self._execute_task(id, task[0], task[1])
        log.info('End executing tasks older than %s', timestamp)

    def _execute_task(self, task_id, args, kw):
        log.info('Enqueuing %s', task_id)
        # XXX No transactions, so we accept the risk of executing a task twice,
        # rather than not executing it at all (with regards to revoke failing).
        self.app.send_task(*args, **kw)
        self.revoke(task_id)

    def revoke(self, task_id):
        try:
            self.backend.delete(task_id)
            log.info('Revoked %s', task_id)
            return True
        except KeyError:
            return False


get_scheduler = Scheduler.from_app
