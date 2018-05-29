Celery Executor
===============

[![PyPI version fury.io](https://badge.fury.io/py/celery-executor.svg)](https://pypi.python.org/pypi/celery-executor/)
[![Build Status](https://travis-ci.org/getninjas/celery-executor.svg?branch=master)](https://travis-ci.org/getninjas/celery-executor)
[![Read The Docs](https://readthedocs.org/projects/celery-executor/badge/?version=latest)](https://celery-executor.readthedocs.io/en/latest/?badge=latest)
[![PyUP](https://pyup.io/repos/github/getninjas/celery-executor/shield.svg)](https://pyup.io/repos/github/getninjas/celery-executor/)

A `concurrent.futures.Executor` implementation using Celery as backend

* Free software: Apache Software License 2.0
* Documentation: https://celery-executor.readthedocs.io.

Features
--------

The package provides a `CeleryExecutor` implementing the interface of
`concurrent.futures.Executor`

```python
>>> from celery_executor.executors import CeleryExecutor
>>> executor = CeleryExecutor()
>>> for result in executor.map(str.upper, ['one', 'two', 'three']):
...     print(result)
'ONE'
'TWO'
'THREE'
```

Credits
-------

This package was created with [Cookiecutter](https://github.com/audreyr/cookiecutter) and the [`audreyr/cookiecutter-pypackage`](https://github.com/audreyr/cookiecutter-pypackage) project template.
