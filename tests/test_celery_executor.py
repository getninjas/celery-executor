#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `celery_executor` package."""

import pytest


from celery_executor.executors import CeleryExecutor, SyncExecutor
from concurrent.futures import ThreadPoolExecutor


@pytest.fixture(scope='session')
def celery_config():
    return {
        'accept_content': ['json', 'pickle'],

        ## The exception inheritance do change if not using 'pickle' serializer
        # See: https://github.com/celery/celery/issues/3586
        # and https://github.com/celery/celery/pull/3592
        'result_serializer': 'pickle',
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
