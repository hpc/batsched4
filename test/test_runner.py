#!/usr/bin/env python3

import itertools
import glob
import os
import os.path
import unittest
import subprocess
import shlex

def test_instance(platform, workload, algo):
    test_name = f'{algo}-{platform.name}-{workload.name}'
    output_dir = os.path.abspath(f'test-out/{test_name}')

    robin_filename = os.path.abspath(f'test-instances/{test_name}.yaml')
    robin_file_content = f'''output-dir: '{output_dir}'
batcmd: batsim -p '{platform.filename}' -w '{workload.filename}' --energy -e '{output_dir}/out'
schedcmd: batsched -v '{algo}'
simulation-timeout: 30
ready-timeout: 5
success-timeout: 10
failure-timeout: 0
'''

    if not os.path.exists(os.path.dirname(robin_filename)):
        os.makedirs(os.path.dirname(robin_filename))

    robin_file = open(robin_filename, "w")
    robin_file.write(robin_file_content)
    robin_file.close()

    ret = subprocess.run(['robin', robin_filename])
    assert ret.returncode == 0

