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
        workload_files = glob.glob('workloads/*.json')
        workloads = [Workload(
            name=basename(workload_file).replace('.json', ''),
            filename=abspath(workload_file)) for workload_file in workload_files]
        metafunc.parametrize('workload', workloads)

    if 'basic_algo_no_param' in metafunc.fixturenames:
        algos = [
            'conservative_bf',
            'easy_bf',
            'easy_bf_fast',
            'fcfs_fast',
            'filler',
            'rejecter',
            'sequencer',
            'waiting_time_estimator'
        ]
        metafunc.parametrize('basic_algo_no_param', algos)

    if 'one_basic_algo' in metafunc.fixturenames:
        algos = [
            'conservative_bf',
        ]
        metafunc.parametrize('one_basic_algo', algos)

    if 'redis_enabled' in metafunc.fixturenames:
        metafunc.parametrize('redis_enabled', [True, False])
