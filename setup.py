#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

import sys
from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [ ]

setup_requirements = [ ]

test_requirements = ['pytest', ]

needs_pytest = {'pytest', 'test', 'ptr'}.intersection(sys.argv)
if needs_pytest:
    setup_requirements += ['pytest-runner']

setup(
    author="Alan Justino da Silva",
    author_email='alan.justino@yahoo.com.br',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    description="A concurrent.futures.Executor implementation using Celery as backend",
    install_requires=requirements,
    license="Apache Software License 2.0",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='celery_executor',
    name='celery-executor',
    packages=find_packages(),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/alanjds/celery-executor',
    version='0.1.0',
    zip_safe=True,
)
