#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `celery_executor` package."""
import time
import six
from pprint import pformat
from concurrent.futures import ThreadPoolExecutor, TimeoutError

import pytest

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


def test_excutor_exception_parity(celery_session_worker):
    tp_exec = ThreadPoolExecutor()
    s_exec = SyncExecutor()
    cl_exec = CeleryExecutor()

    operations = ['/nonexistentfile', '/anothernonexistentfile']

    with pytest.raises(IOError):
        list(map(open, operations))

    with pytest.raises(IOError):
        list(tp_exec.map(open, operations))

    with pytest.raises(IOError):
        list(s_exec.map(open, operations))

    with pytest.raises(IOError):
        list(cl_exec.map(open, operations))


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
