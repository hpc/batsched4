#!/usr/bin/env python3
import itertools
import json
import glob
import os
import os.path
import unittest

from robin_helper import *

def init_instance(test_name):
    output_dir = os.path.abspath(f'test-out/{test_name}')
    robin_filename = os.path.abspath(f'test-instances/{test_name}.yaml')
    batconf_filename = f'{output_dir}/batconf.json'
    schedconf_filename = f'{output_dir}/schedconf.json'

    create_dir_rec_if_needed(output_dir)

    return (output_dir, robin_filename, batconf_filename, schedconf_filename)

def test_basic_algo_no_param(platform, workload, basic_algo_no_param):
    test_name = f'{basic_algo_no_param}-{platform.name}-{workload.name}'
    output_dir, robin_filename, _, _ = init_instance(test_name)

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

def test_easy_bf_plot_llh(platform, workload):
    algo = 'easy_bf_plot_liquid_load_horizon'
    test_name = f'{algo}-{platform.name}-{workload.name}'
    output_dir, robin_filename, _, schedconf_filename = init_instance(test_name)

    batcmd = gen_batsim_cmd(platform.filename, workload.filename, output_dir, "--energy")

    schedconf_content = {
        "trace_output_filename": f'{output_dir}/batsched_llh.trace'
    }
    write_file(schedconf_filename, json.dumps(schedconf_content))

    instance = RobinInstance(output_dir=output_dir,
        batcmd=batcmd,
        schedcmd=f"batsched -v '{algo}' --variant_options_filepath '{schedconf_filename}'",
        simulation_timeout=30, ready_timeout=5,
        success_timeout=10, failure_timeout=0
    )

    instance.to_file(robin_filename)
    ret = run_robin(robin_filename)
    assert ret.returncode == 0

def test_redis(platform, workload, one_basic_algo, redis_enabled):
    test_name = f'{one_basic_algo}-{platform.name}-{workload.name}'
    output_dir, robin_filename, batconf_filename, _ = init_instance(test_name)

    batcmd = gen_batsim_cmd(platform.filename, workload.filename, output_dir,
        f"--config-file '{batconf_filename}'")

    batconf_content = {
        "redis": {
            "enabled": redis_enabled
        }
    }
    write_file(batconf_filename, json.dumps(batconf_content))

    instance = RobinInstance(output_dir=output_dir,
        batcmd=batcmd,
        schedcmd=f"batsched -v '{one_basic_algo}'",
        simulation_timeout=30, ready_timeout=5,
        success_timeout=10, failure_timeout=0
    )

    instance.to_file(robin_filename)
    ret = run_robin(robin_filename)
    assert ret.returncode == 0
