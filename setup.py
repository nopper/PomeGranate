#!/usr/bin/env python

from setuptools import setup

setup(
    name='PomeGranate',
    version='1.0',
    description='Simple fault-tolerant MapReduce framework for Python/MPI',
    author='Francesco Piccinno',
    author_email='stack.box@gmail.com',
    url='http://github.com/nopper/pomegranate',
    packages=['pomegranate'],
    package_dir={'pomegranate': 'src'},
    package_data={'pomegranate': ['templates/*.html']},
    install_requires=[
        'mpi4py',
        'jinja2'
    ],
    scripts=[
        'scripts/pmgr-server',
        'scripts/pmgr-client',
    ],
)
