#!/usr/bin/env python3
import glob
import pytest
import subprocess
from collections import namedtuple
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

    if 'monitoring_period' in metafunc.fixturenames:
        metafunc.parametrize('monitoring_period', [600])

    if 'inertial_function' in metafunc.fixturenames:
        metafunc.parametrize('inertial_function', ['x2', 'p1'])

    if 'idle_time_to_sedate' in metafunc.fixturenames:
        metafunc.parametrize('idle_time_to_sedate', [0, 120])

    if 'sedate_idle_on_classical_events' in metafunc.fixturenames:
        metafunc.parametrize('sedate_idle_on_classical_events', [True, False])

    if 'allow_future_switches' in metafunc.fixturenames:
        metafunc.parametrize('allow_future_switches', [True])

    if 'upper_llh_threshold' in metafunc.fixturenames:
        metafunc.parametrize('upper_llh_threshold', [60])

@pytest.fixture(scope="session", autouse=True)
def manage_redis_server(request):
    print('Trying to run a redis-server...')
    proc = subprocess.Popen('redis-server', stdout=subprocess.PIPE)
    try:
        out, _ = proc.communicate(timeout=1)
        if 'Address already in use' in str(out):
            print("Could not run redis-server (address already in use).")
            print("Assuming that the process using the TCP port is another redis-server instance and going on.")
        else:
            raise Exception("Could not run redis-server (unhandled reason), aborting.")
    except subprocess.TimeoutExpired:
        print('redis-server has been running for 1 second.')
        print('Assuming redis-server has started successfully and going on.')

    def on_finalize():
        print('Killing the spawned redis-server (if any)...')
        proc.kill()
    request.addfinalizer(on_finalize)
