from celery_longterm_scheduler import backend
import celery.bin.base
import contextlib
import fcntl
import logging
import os
import pendulum


log = logging.getLogger(__name__)


class Scheduler(object):
    """Main scheduler functionality:

    :store: schedule tasks for later
    :revoke: revoke scheduled tasks
    :execute_pending: execute scheduled tasks due by a given timestamp

    Clients should use ``get_scheduler(app)`` with their celery app instance
    to get hold of the corresponding Scheduler instance.
    """

    CONF_KEY = '__longterm_scheduler_backend'

    def __init__(self, app):
        self.app = app
        # Singleton behaviour
        if self.CONF_KEY not in app.conf:
            app.conf[self.CONF_KEY] = backend.by_url(
                app.conf['longterm_scheduler_backend'], app)
        self.backend = app.conf[self.CONF_KEY]

    @classmethod
    def from_app(cls, app):
        return cls(app)

    def store(self, timestamp, task_id, args, kw):
        """Schedules the task (represented by the ``args`` and ``kw`` of the
        postponed send_task() call) under ``task_id`` and ``timestamp``.

        :param timestamp: timezone-aware datetime
        :param task_id: string, the task id, can be used in revoke()
        :param args: tuple, positional arguments for the task
        :param kw: dict, keyword arguments for the task
        """
        self.backend.set(timestamp, task_id, args, kw)

    def execute_pending(self, timestamp):
        """Looks up scheduled tasks that are due on or before ``timestamp``,
        creates normal celery tasks for them, and removes them from the
        scheduler storage.

        :param timestamp: timezone-aware datetime
        """
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
        """Removes the task scheduled by ``store(task_id)`` from scheduler
        storage.

        :returns: True if ``task_id`` was found and removed, False otherwise"""
        try:
            self.backend.delete(task_id)
            log.info('Revoked %s', task_id)
            return True
        except KeyError:
            return False


get_scheduler = Scheduler.from_app


class Command(celery.bin.base.Command):
    """The subcommand ``celery longterm_scheduler`` executes scheduled tasks
    that are due on or before a given time (default: now), by creating normal
    celery tasks for them.
    """

    DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

    def add_arguments(self, parser):
        parser.add_argument(
            '--timestamp',
            help='Execute tasks older/equal to TIMESTAMP, default: now')
        parser.add_argument(
            '--lockfile',
            help='Path to lockfile, to prevent multiple simultaneous runs')

    def run(self, timestamp=None, lockfile=None, **kw):
        self.app.log.setup(
            logging.WARNING if kw.get('quiet') else logging.INFO)
        if timestamp is None:
            timestamp = 'now'
        # The `tz` parameter applies only if no timezone information is
        # present in the string -- which is precisely what we want here;
        # tz=None means use the locale's timezone.
        timestamp = pendulum.parse(timestamp, tz=None)
        if lockfile:
            with locked(lockfile):
                get_scheduler(self.app).execute_pending(timestamp)
        else:
            get_scheduler(self.app).execute_pending(timestamp)


@contextlib.contextmanager
def locked(filename):
    """Context manager that acquires a file-based lock or raises RuntimError if
    already locked.
    """
    lockfile = open(filename, 'a+')
    try:
        fcntl.lockf(lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        raise RuntimeError('Another process is already running')
    # Publishing the process id is handy for debugging.
    lockfile.seek(0)
    lockfile.truncate()
    lockfile.write('%s\n' % os.getpid())
    lockfile.flush()
    try:
        yield
    finally:
        lockfile.seek(0)
        lockfile.truncate()
        lockfile.close()  # This implicitly unlocks.
