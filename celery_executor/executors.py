from concurrent.futures import Future, Executor, as_completed
from threading import Lock, Thread
import logging
import time

from celery import shared_task

logger = logging.getLogger(__name__)


@shared_task(serializer='pickle')
def _celery_call(func, *args, **kwargs):
    return func(*args, **kwargs)


class CeleryFuture(Future):
    def __init__(self, asyncresult):
        self._ar = asyncresult
        super(CeleryFuture, self).__init__()

    def cancel(self):
        """Cancel the future if possible.
        Returns True if the future was cancelled, False otherwise. A future
        cannot be cancelled if it is running or has already completed.
        """
        ## Note that this method does not call super()
        # Its because the place where ._ar.revoke() should be called is
        # in the middle of the Future.cancel() code.
        # Solved by copying and adapting from stdlib
        # See: https://github.com/python/cpython/blob/087570af6d5d39b51bdd5e660a53903960e58678/Lib/concurrent/futures/_base.py#L352-L369
        with self._condition:
            if self._state in ['RUNNING', 'FINISHED']:
                return False

            if self._state in ['CANCELLED', 'CANCELLED_AND_NOTIFIED']:
                return True

            # Not running and not canceled. May be possible to cancel!
            self._ar.ready()  # Triggers an update check
            if self._ar.state != 'REVOKED':
                self._ar.revoke()
                self._ar.ready()

            # Celery task should be REVOKED now. Otherwise may be not possible revoke it.
            if self._ar.state == 'REVOKED':
                self._state = 'CANCELLED'
                self._condition.notify_all()
            else:
                return False

        # Was .revoke()d with success!
        self._invoke_callbacks()
        return True


class CeleryExecutor(Executor):
    def __init__(self,
                    predelay=None,
                    postdelay=None,
                    applyasync_kwargs=None,
                    update_delay=0.1,
                ):
        """
        Executor implementation using a celery caller `_celery_call` wrapper
        around the submitted tasks.

        Args:
            predelay: Will trigger before the `.apply_async` internal call
            postdelay: Will trigger before the `.apply_async` internal call
            applyasync_kwargs: Options passed to the `.apply_async()` call
            update_delay: Delay time between checks for Future state changes
        """
        self._predelay = predelay
        self._postdelay = postdelay
        self._applyasync_kwargs = applyasync_kwargs or {}
        self._update_delay = update_delay
        self._shutdown = False
        self._shutdown_lock = Lock()
        self._futures = {}
        self._monitor_started = False
        self._monitor_stopping = False
        self._monitor = Thread(target=self._update_futures)
        self._monitor.setDaemon(True)

    def _update_futures(self):
        while True:
            time.sleep(self._update_delay)  # Not-so-busy loop
            if self._monitor_stopping:
                return

            for fut, ar in tuple(self._futures.items()):
                if fut._state in ('FINISHED', 'CANCELLED_AND_NOTIFIED'):
                    # This Future is set and done. Nothing else to do.
                    self._futures.pop(fut)
                    continue

                ar.ready()   # Just trigger the AsyncResult state update check

                if ar.state == 'REVOKED':
                    logger.debug('Celery task "%s" canceled.', ar.id)
                    if not fut.cancelled():
                        assert fut.cancel(), 'Future was not running but failed to be cancelled'
                        fut.set_running_or_notify_cancel()
                    # Future is 'CANCELLED'

                elif ar.state in ('RUNNING', 'RETRY'):
                    logger.debug('Celery task "%s" running.', ar.id)
                    if not fut.running():
                        fut.set_running_or_notify_cancel()
                    # Future is 'RUNNING'

                elif ar.state == 'SUCCESS':
                    logger.debug('Celery task "%s" resolved.', ar.id)
                    fut.set_result(ar.get())
                    # Future is 'FINISHED'

                elif ar.state == 'FAILURE':
                    logger.debug('Celery task "%s" resolved with error.', ar.id)
                    fut.set_exception(ar.result)
                    # Future is 'FINISHED'

                # else:  # ar.state in [RECEIVED, STARTED, REJECTED, RETRY]
                #     pass

    def submit(self, fn, *args, **kwargs):
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError('cannot schedule new futures after shutdown')

            if not self._monitor_started:
                self._monitor.start()
                self._monitor_started = True

            if self._predelay:
                self._predelay(fn, *args, **kwargs)
            asyncresult = _celery_call.apply_async((fn,) + args, kwargs,
                                                **self._applyasync_kwargs)
            if self._postdelay:
                self._postdelay(asyncresult)

            future = CeleryFuture(asyncresult)
            self._futures[future] = asyncresult
            return future

    def shutdown(self, wait=True):
        with self._shutdown_lock:
            self._shutdown = True
            for fut in self._futures:
                fut.cancel()

        if wait:
            for fut in as_completed(self._futures):
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
