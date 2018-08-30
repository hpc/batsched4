#!/usr/bin/env python3
from collections import namedtuple
import glob
from os.path import abspath, basename

Workload = namedtuple('Workload', ['name', 'filename'])
Platform = namedtuple('Platform', ['name', 'filename'])

def pytest_generate_tests(metafunc):
    if 'platform' in metafunc.fixturenames:
        platform_files = glob.glob('platforms/*.xml')
        platforms = [Platform(
            name=basename(platform_file).replace('.xml', ''),
            filename=abspath(platform_file)) for platform_file in platform_files]
        metafunc.parametrize('platform', platforms)

    if 'workload' in metafunc.fixturenames:
        # workload_files = glob.glob('workloads/*.json')
        # workloads = [Workload(
        #     name=basename(workload_file).replace('.json', ''),
        #     filename=workload_file) for workload_file in workload_files]
        workloads = [Workload('mixed', abspath('workloads/mixed.json'))]
        metafunc.parametrize('workload', workloads)

    if 'algo' in metafunc.fixturenames:
        algos = [
            'filler',
            'easy_bf'
        ]
        metafunc.parametrize('algo', algos)
