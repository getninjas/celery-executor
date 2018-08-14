#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `celery_executor` package."""
import time
import six
from pprint import pformat
from concurrent.futures import ThreadPoolExecutor, TimeoutError, CancelledError

import pytest
from kombu import Queue

if six.PY2:
    # Tests will work only with dill + pure-python pickle
    import dill
    import pickle
    import kombu.serialization as _serialization

    from io import BytesIO
    def pickle_loads(s, load=pickle.load):
        # used to support buffer objects
        return load(BytesIO(s))

    _serialization.pickle = pickle
    _serialization.pickle_load = pickle.load
    _serialization.unpickle = pickle_loads
    _serialization.register_pickle()

from celery_executor.executors import CeleryExecutor, SyncExecutor


@pytest.fixture(scope='session')
def celery_config():
    return {
        'accept_content': ['json', 'pickle'],

        ## The exception inheritance do change if not using 'pickle' serializer
        # See: https://github.com/celery/celery/issues/3586
        # and https://github.com/celery/celery/pull/3592
        'result_serializer': 'pickle',

        'worker_concurrency': 1,
        'worker_prefetch_multiplier': 1,
        'worker_lost_wait': 60,
        'worker_send_task_events': True,
        'task_acks_late': True,
        'task_queues': [Queue('celery'), Queue('retry')],
    }


def test_excutors_parity(celery_session_worker):
    tp_exec = ThreadPoolExecutor()
    s_exec = SyncExecutor()
    cl_exec = CeleryExecutor()

    operations = ['first', 'second']

    map_results = list(sorted(map(str.upper, operations)))
    s_results = list(sorted(s_exec.map(str.upper, operations)))
    tp_results = list(sorted(tp_exec.map(str.upper, operations)))
    cl_results = list(sorted(cl_exec.map(str.upper, operations)))

    assert map_results == s_results == tp_results == cl_results

    tp_exec.shutdown(wait=True)
    s_exec.shutdown(wait=True)
    cl_exec.shutdown(wait=True)


@pytest.mark.parametrize("executor_class", [ThreadPoolExecutor, SyncExecutor, CeleryExecutor])
def test_executors_shutdown_parity(executor_class, celery_session_worker):
    executor = executor_class()

    executor.shutdown()
    with pytest.raises(RuntimeError):
        executor.submit(pow, 2, 5)


@pytest.mark.parametrize("executor_class", [ThreadPoolExecutor, SyncExecutor, CeleryExecutor])
def test_executor_exception_parity(executor_class, celery_session_worker):
    executor = executor_class()

    operations = ['/nonexistentfile', '/anothernonexistentfile']

    with pytest.raises(IOError):
        list(map(open, operations))

    with pytest.raises(IOError):
        list(executor.map(open, operations))

    executor.shutdown(wait=True)


def test_futures_parity(celery_session_worker):
    tp_exec = ThreadPoolExecutor(max_workers=1)
    cel_exec = CeleryExecutor()
    # SyncExecutor Futures will not be tested as they are standard Python ones.

    operations = [2, 3]  # The last will be canceled.
    func = time.sleep

    tp_futures = []
    cel_futures = []

    for op in operations:
        tp_futures.append(tp_exec.submit(func, op))
        cel_futures.append(cel_exec.submit(func, op))

    time.sleep(10)   # First should had finished. Second no.

    states = []
    assert len(tp_futures) == len(cel_futures)
    for tp_fut, cel_fut in zip(tp_futures, cel_futures):
        states.append(  # A tuple with (tp, cel) results
            (_collect_state(tp_fut),
             _collect_state(cel_fut)),
        )

    for tp_fut_state, cel_fut_state in states:
        assert pformat(tp_fut_state) == pformat(cel_fut_state)

    states = []
    for tp_fut, cel_fut in zip(tp_futures, cel_futures):
        tp_cancel_result = tp_fut.cancel()
        cel_cancel_result = cel_fut.cancel()
        states.append(  # A tuple with (tp, cel) results
            ((tp_cancel_result, _collect_state(tp_fut)),
             (cel_cancel_result, _collect_state(cel_fut))),
        )

    for tp_fut_state, cel_fut_state in states:
        assert tp_fut_state == cel_fut_state

    tp_exec.shutdown(wait=True)
    cel_exec.shutdown(wait=True)


## Could not force a REVOKE on celery future! (or: I could not.)
# def test_future_cancel_parity(celery_session_worker):
#     tp_exec = ThreadPoolExecutor(max_workers=1)
#     cel_exec = CeleryExecutor()

#     tp_exec.submit(time.sleep, 10)  # Make the executor busy
#     tp_future = tp_exec.submit(time.sleep, 10)
#     tp_future.cancel()

#     cel_exec.submit(time.sleep, 10)
#     cel_future = cel_exec.submit(time.sleep, 10)
#     cel_future.cancel()

#     assert tp_future.cancelled() == cel_future.cancelled()
#     assert tp_future.exception() == cel_future.exception()


def test_future_exception_parity(celery_session_worker):
    tp_exec = ThreadPoolExecutor(max_workers=1)
    cel_exec = CeleryExecutor()

    tp_future = tp_exec.submit(open, '/nonexistingfile')
    cel_future = cel_exec.submit(open, '/nonexistingfile')

    assert str(tp_future.exception()) == str(cel_future.exception())

    tp_future = tp_exec.submit(time.sleep, 5)
    cel_future = cel_exec.submit(time.sleep, 5)

    with pytest.raises(TimeoutError) as tp_err:
        tp_future.exception(1)

    with pytest.raises(TimeoutError) as cel_err:
        cel_future.exception(1)

    assert tp_err.type == cel_err.type
    assert str(tp_err.value) == str(cel_err.value)

    tp_exec.shutdown(wait=True)
    cel_exec.shutdown(wait=True)


@pytest.mark.parametrize("executor_class", [ThreadPoolExecutor, SyncExecutor, CeleryExecutor])
def test_hang_issue12364(executor_class, celery_session_worker):
    executor = executor_class()
    futures = [executor.submit(time.sleep, 0.1) for _ in range(50)]
    executor.shutdown()
    for f in futures:
        try:
            f.result()
        except CancelledError:
            pass


@pytest.mark.parametrize("executor_class", [ThreadPoolExecutor, SyncExecutor, CeleryExecutor])
def test_del_shutdown(executor_class, celery_session_worker):
    executor = executor_class()
    executor.map(abs, range(-5, 5))
    del executor


def _collect_state(future):
    state = {
        'cancelled': future.cancelled(),
        'running': future.running(),
        'done': future.done(),
    }
    try:
        state['result'] = future.result(timeout=0)
    except Exception as err:
        state['result'] = err

    try:
        state['exception'] = future.exception(timeout=0)
    except Exception as err:
        state['exception'] = err

    state['_state'] = future._state
    return state


def change_queue_function(call_type='regular'):
    if call_type != 'retry':
        raise RuntimeError()
    return call_type


def test_change_queue_on_exception(celery_session_worker):
    test_retry_kwargs = {'kwargs': {'call_type': 'retry'}, 'countdown': 1}
    with CeleryExecutor(retry_kwargs=test_retry_kwargs, retry_queue='retry') as executor:
        future = executor.submit(change_queue_function)
        result = future.result(timeout=3)
        assert result == 'retry'


def test_fail_change_queue_on_exception(celery_session_worker):
    test_retry_kwargs = {'kwargs': {'call_type': 'fail'}, 'countdown': 1, 'max_retries': 3}
    with CeleryExecutor(retry_kwargs=test_retry_kwargs, retry_queue='retry') as executor:
        future = executor.submit(change_queue_function)
        with pytest.raises(RuntimeError):
            result = future.result(timeout=6)
