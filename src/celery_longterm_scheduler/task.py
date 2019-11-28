from __future__ import unicode_literals
import celery
import celery.utils
import celery_longterm_scheduler


class Task(celery.Task):
    """Task subclass that adds special behaviour when ``apply_async()`` is
    called with an ``eta`` argument: the job is not created immediately, but
    rather stored in a separate scheduler storage.

    This still returns a normal ``AsyncResult`` object, but only getting the
    ``id`` is supported on it. This ID can be passed to
    ``celery_longterm_scheduler.revoke()`` to remove the scheduled job from
    storage.
    """

    def apply_async(self, args=None, kwargs=None, task_id=None, producer=None,
                    link=None, link_error=None, shadow=None, **options):
        if options.get('eta') is not None:
            timestamp = options.pop('eta')

            # copy&paste from celery.app.task.Task.apply_async()
            if self.__v2_compat__:
                shadow = shadow or self.shadow_name(
                    self(), args, kwargs, options)
            else:
                shadow = shadow or self.shadow_name(args, kwargs, options)

            preopts = self._get_exec_options()
            options = dict(preopts, **options) if options else preopts

            options.setdefault('ignore_result', self.ignore_result)
            if self.priority:
                options.setdefault('priority', self.priority)
            # end copy&paste

            return self._schedule(
                timestamp,
                args=args, kwargs=kwargs, task_id=task_id, producer=producer,
                link=link, link_error=link_error, shadow=shadow, **options)
        else:
            return super(Task, self).apply_async(
                args=args, kwargs=kwargs, task_id=task_id, producer=producer,
                link=link, link_error=link_error, shadow=shadow, **options)

    def _schedule(self, timestamp, **kw):
        # Store parameters apply_async() passes to app.send_task() in addition
        # to its own **kw.
        kw['task_type'] = self
        # We don't set result_cls, since serializing instancemethods is a pain
        # and the additional settings of self.AsyncResult compared to
        # app.AsyncResult don't make a difference _inside_ send_task, so we
        # don't actually need it. And for the return value of apply_async we
        # call it ourselves anyway, see below.
        # kw['result_cls'] = self.AsyncResult

        # We use the celery task_id also for our scheduler storage; this is
        # mostly for integration purposes, e.g. so that other Task subclasses
        # can be in control of the task_id and still work when inheriting us.
        if not kw.get('task_id'):
            kw['task_id'] = celery.utils.gen_unique_id()

        scheduler = celery_longterm_scheduler.get_scheduler(self.app)
        scheduler.store(timestamp, kw['task_id'], (self.name,), kw)
        return self.AsyncResult(kw['task_id'])
