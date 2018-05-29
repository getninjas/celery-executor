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
[`concurrent.futures.Executor`](https://docs.python.org/3/library/concurrent.futures.html#executor-objects)

```python
>>> from celery_executor.executors import CeleryExecutor
>>> executor = CeleryExecutor()
>>> for result in executor.map(str.upper, ['one', 'two', 'three']):
...     print(result)
ONE
TWO
THREE
```

Beware that the `Executor.map()` interface can yield the results out of order,
if later ones got to finish first.

Caveats
-------

This executor frees the developer to the burden of mark every single task
function with the Celery decorators, and to import such tasks on the Worker
beforehand. But does not frees from sending the code to the Worker.

The function sent to `CeleryExecutor.map()` should be pickable on the client
(caller of `.map()` or `.submit()`) and should be unpickable on the Celery
Worker handling the "Task" sent. Is not possible to send lambdas for example.

As Celery assumes that is to the developer to put the needed code on the Worker,
be sure that the function/partial code sent to `CeleryExecutor` to exist on the
Worker.

To Be Done
----------

- [ ] Document the `CeleryExecutor.__init__()` nonstandard extra options `predelay`, `postdelay` and `applyasync_kwargs`.
- [ ] Test behaviours of canceling a Task when canceling a Future
- [ ] Test behaviours of shutting down executors and trying to send new tasks
- [ ] Find a way to test the RUNNING state of Celery Tasks, as its events are not propagated by the test worker Celery provides

Credits
-------

This package was created with [Cookiecutter](https://github.com/audreyr/cookiecutter) and the [`audreyr/cookiecutter-pypackage`](https://github.com/audreyr/cookiecutter-pypackage) project template.
