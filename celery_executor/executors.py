from concurrent.futures import Future, Executor, as_completed
from concurrent.futures._base import RUNNING, FINISHED, CANCELLED, CANCELLED_AND_NOTIFIED
from threading import Lock, Thread
import logging
import time

from celery import shared_task

logger = logging.getLogger(__name__)


@shared_task(bind=True, serializer='pickle', track_started=True, retry_backoff=True)
def _celery_call(task, func, metadata, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as exc:
        retry_kwargs = (metadata or {}).get('retry_kwargs', {})
        raise task.retry(exc=exc, **retry_kwargs)


class CeleryExecutorFuture(Future):
    def __init__(self, asyncresult):
        self._ar = asyncresult
        super(CeleryExecutorFuture, self).__init__()

    def __del__(self):
        self._ar.forget()
        del self._ar

    def cancel(self):
        """Cancel the future if possible.
        Returns True if the future was cancelled, False otherwise. A future
        cannot be cancelled if it is running or has already completed.
        """
        with self._condition:
            if self._state in [RUNNING, FINISHED, CANCELLED, CANCELLED_AND_NOTIFIED]:
                return super(CeleryExecutorFuture, self).cancel()

            # Not running and not canceled. May be possible to cancel!
            self._ar.ready()  # Triggers an update check
            if self._ar.state != 'REVOKED':
                self._ar.revoke()
                self._ar.ready()

            # Celery task should be REVOKED now. Otherwise may be not possible revoke it.
            if self._ar.state == 'REVOKED':
                result = super(CeleryExecutorFuture, self).cancel()
                assert result is True, 'Please open an issue on Github: Upstream implementation changed?'
            else:
                # Is not running nor revoked nor finished :/
                # The revoke() had not produced effect: Task is probable not on a worker, then not revoke-able.
                # Setting as RUNNING to inibit super() to cancel the Future, then putting back.
                initial_state = self._state
                self._state = RUNNING
                result = super(CeleryExecutorFuture, self).cancel()
                assert result is False, 'Please open an issue on Github: Upstream implementation changed?'
                self._state = initial_state

            return result


class CeleryExecutor(Executor):
    def __init__(self, predelay=None, postdelay=None, applyasync_kwargs=None,
                 retry_kwargs=None, retry_queue='', update_delay=0.1):
        """
        Executor implementation using a celery caller `_celery_call` wrapper
        around the submitted tasks.

        Args:
            predelay: Will trigger before the `.apply_async` internal call
            postdelay: Will trigger before the `.apply_async` internal call
            applyasync_kwargs: Options passed to the `.apply_async()` call
            retry_kwargs: Options passed to the `.retry()` call on errors
            retry_queue: Sugar to set an alternative queue specially for errors
            update_delay: Delay time between checks for Future state changes
        """
        # Options about calling the Task
        self._predelay = predelay
        self._postdelay = postdelay
        self._applyasync_kwargs = applyasync_kwargs or {}
        self._retry_kwargs = retry_kwargs or {}
        if retry_queue:
            self._retry_kwargs['queue'] = retry_queue
            self._retry_kwargs.setdefault('max_retries', 1)
        self._retry_kwargs.setdefault('max_retries', 0)

        # Options about managing this Executor flow
        self._update_delay = update_delay
        self._shutdown = False
        self._shutdown_lock = Lock()
        self._futures = set()
        self._monitor_started = False
        self._monitor_stopping = False
        self._monitor = Thread(target=self._update_futures)
        self._monitor.setDaemon(True)

    def _update_futures(self):
        while True:
            time.sleep(self._update_delay)  # Not-so-busy loop
            if self._monitor_stopping:
                return

            for fut in tuple(self._futures):
                if fut._state in (FINISHED, CANCELLED_AND_NOTIFIED):
                    # This Future is set and done. Nothing else to do.
                    self._futures.remove(fut)
                    continue

                ar = fut._ar
                ar.ready()  # Just trigger the AsyncResult state update check

                if ar.state == 'REVOKED':
                    logger.debug('Celery task "%s" canceled.', ar.id)
                    if not fut.cancelled():
                        assert fut.cancel(), 'Future was not running but failed to be cancelled'
                        fut.set_running_or_notify_cancel()
                    # Future is CANCELLED

                elif ar.state in ('RUNNING', 'RETRY'):
                    logger.debug('Celery task "%s" running.', ar.id)
                    if not fut.running():
                        fut.set_running_or_notify_cancel()
                    # Future is RUNNING

                elif ar.state == 'SUCCESS':
                    logger.debug('Celery task "%s" resolved.', ar.id)
                    fut.set_result(ar.get(disable_sync_subtasks=False))
                    # Future is FINISHED

                elif ar.state == 'FAILURE':
                    logger.debug('Celery task "%s" resolved with error.', ar.id)
                    fut.set_exception(ar.result)
                    # Future is FINISHED

                # else:  # ar.state in [RECEIVED, STARTED, REJECTED, RETRY]
                #     pass

    def submit(self, fn, *args, **kwargs):
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError('cannot schedule new futures after shutdown')

            if not self._monitor_started:
                self._monitor.start()
                self._monitor_started = True

            metadata = {
                'retry_kwargs': self._retry_kwargs.copy()
            }

            if self._predelay:
                self._predelay(fn, *args, **kwargs)
            asyncresult = _celery_call.apply_async((fn, metadata) + args, kwargs,
                                                   **self._applyasync_kwargs)
            if self._postdelay:
                self._postdelay(asyncresult)

            future = CeleryExecutorFuture(asyncresult)
            self._futures.add(future)
            return future

    def shutdown(self, wait=True):
        with self._shutdown_lock:
            self._shutdown = True
            for fut in self._futures:
                fut.cancel()

        if wait:
            for _ in as_completed(self._futures):
                pass

            self._monitor_stopping = True
            try:
                self._monitor.join()
            except RuntimeError:
                # Thread never started. Cannot join
                pass


class SyncExecutor(Executor):
    """
    Executor that does the job syncronously.

    All the returned Futures will be already resolved. This executor is intended
    to be mainly used on tests or to keep internal API conformance with the
    Executor interface.

    Based on a snippet from https://stackoverflow.com/a/10436851/798575
    """

    def __init__(self, lock=Lock):
        self._shutdown = False
        self._shutdown_lock = lock()

    def submit(self, fn, *args, **kwargs):
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError('cannot schedule new futures after shutdown')

            f = Future()
            try:
                result = fn(*args, **kwargs)
            except BaseException as e:
                f.set_exception(e)
            else:
                f.set_result(result)

            return f

    def shutdown(self, wait=True):
        with self._shutdown_lock:
            self._shutdown = True
