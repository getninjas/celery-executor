from concurrent.futures import Future, Executor, CancelledError, TimeoutError as FutureTimeoutError
from threading import Lock
import logging
try:
    from collections.abc import Callable
except ImportError:
    from collections import Callable    # Py27

from future.utils import raise_with_traceback

from celery import shared_task
from celery.exceptions import TimeoutError as CeleryTimeoutError

logger = logging.getLogger(__name__)


@shared_task(serializer='pickle')
def _celery_call(func, *args, **kwargs):
    return func(*args, **kwargs)


class CeleryExecutorFuture(Future):
    def __init__(self, asyncresult, *args, **kwargs):
        super(CeleryExecutorFuture, self).__init__(*args, **kwargs)
        self._ar = asyncresult
        asyncresult.then(self._callback, on_error=self._error)
        self._ar.ready()   # Just trigger the state update check
    
    def __repr__(self):
        self._ar.ready()   # Triggers an update check
        return super(CeleryExecutorFuture, self).__repr__()

    def cancel(self):
        self._ar.revoke()
        self._ar.ready()   # Triggers an update check

        if self._ar.state == 'REVOKED':
            return True
        else:
            return False

    def cancelled(self):
        self._ar.ready()   # Triggers an update check
        return bool(self._ar.state == 'REVOKED')

    def running(self):
        self._ar.ready()   # Triggers an update check
        return bool(self._ar.state in ['STARTED', 'RETRY'])
    
    def done(self):
        self._ar.ready()   # Triggers an update check
        return bool(self._ar.state in ['SUCCESS', 'REVOKED', 'FAILURE'])

    def result(self, timeout=None):
        self._ar.ready()   # Triggers an update check

        if self._ar.state == 'REVOKED':
            raise CancelledError()

        if timeout == 0:    # On Celery, 0 == None
            timeout = 0.000000000001

        try:
            return self._ar.wait(timeout=timeout)  # Will (re)raise exception if occurred
        except CeleryTimeoutError as err:
            raise_with_traceback(FutureTimeoutError())
    
    def exception(self, timeout=None):
        if timeout == 0:    # On Celery, 0 == None
            timeout = 0.000000000001

        try:
            self.result(timeout=timeout)   # Will trigger the update check
        except (FutureTimeoutError, CancelledError):
            raise
        except BaseException as err:
            return err  # Got the exception raised into the Future call
        return None  # No exception raised
    
    def _callback(self, asyncresult):
        logger.debug('Celery task "%s" resolved.', asyncresult.id)
        self.set_result(asyncresult.result)
    
    def _error(self, asyncresult):
        logger.debug('Celery task "%s" resolved with error.', asyncresult.id)
        self.set_exception(asyncresult.result)

            
class CeleryExecutor(Executor):
    def __init__(self,
                    predelay=lambda fn,*args,**kwargs:None,
                    postdelay=lambda asyncresult:None,
                    applyasync_kwargs=None,
                ):
        """
        Executor implementation using a celery caller `_celery_call` wrapper
        around the submitted tasks.

        Args:
            predelay: Will trigger before the `.apply_async` internal call
            postdelay: Will trigger before the `.apply_async` internal call
            applyasync_kwargs: Options passed to the `.apply_async()` call 
        """
        self._predelay = predelay
        self._postdelay = postdelay
        self._applyasync_kwargs = applyasync_kwargs or {}
        self._shutdown = False
    
    def submit(self, fn, *args, **kwargs):
        if self._shutdown:
            raise RuntimeError('cannot schedule new futures after shutdown')
        self._predelay(fn, *args, **kwargs)
        asyncresult = _celery_call.apply_async((fn,) + args, kwargs,
                                               **self._applyasync_kwargs)
        self._postdelay(asyncresult)
        return CeleryExecutorFuture(asyncresult)

    def shutdown(self, wait=True):
        self._shutdown = True
        # It is faked for now.


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
