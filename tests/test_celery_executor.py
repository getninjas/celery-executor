#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `celery_executor` package."""
import time
from pprint import pformat
from concurrent.futures import ThreadPoolExecutor

import pytest

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

    with pytest.raises(OSError):
        list(map(open, operations))

    with pytest.raises(OSError):
        list(tp_exec.map(open, operations))

    with pytest.raises(OSError):
        list(s_exec.map(open, operations))

    with pytest.raises(OSError):
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
