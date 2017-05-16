#!/usr/bin/env python3

from setuptools import setup, find_packages

setup(name='rat',
        version='0.0.1',
        description='Experiments',
        author='yk',
        packages=find_packages(exclude=("bin",)),
        install_requires=['numpy', 'scipy', 'rq', 'pymongo', 'rcfile', 'terminaltables', 'sh', 'tqdm'],
        )
