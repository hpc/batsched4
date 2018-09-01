#!/usr/bin/env python3
import itertools
import glob
import os
import os.path
import unittest

from robin_helper import *

def gen_test_vars(test_name):
    output_dir = os.path.abspath(f'test-out/{test_name}')
    robin_filename = os.path.abspath(f'test-instances/{test_name}.yaml')
    return (output_dir, robin_filename)

def test_basic_algo_no_param(platform, workload, basic_algo_no_param):
    test_name = f'{basic_algo_no_param}-{platform.name}-{workload.name}'
    output_dir, robin_filename = gen_test_vars(test_name)

    batcmd = gen_batsim_cmd(platform.filename, workload.filename, output_dir, "")
    instance = RobinInstance(output_dir=output_dir,
        batcmd=batcmd,
        schedcmd=f"batsched -v '{basic_algo_no_param}'",
        simulation_timeout=30, ready_timeout=5,
        success_timeout=10, failure_timeout=0
    )

    instance.to_file(robin_filename)
    ret = run_robin(robin_filename)
    assert ret.returncode == 0

