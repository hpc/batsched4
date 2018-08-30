#!/usr/bin/env python3

import itertools
import glob
from os.path import abspath, basename
import unittest
import subprocess
import shlex

def test_instance(platform, workload, algo):
    output_dir = f'test-out/{algo}-{platform.name}-{workload.name}'
    robin_cmd = f'''robin --output-dir={output_dir} \\
      --batcmd='batsim -p {platform.filename} -w {workload.filename} --energy -e {output_dir}/out' \\
      --schedcmd='batsched -v {algo}' \\
      --simulation-timeout=30 \\
      --success-timeout=10 \\
      --failure-timeout=0
'''
    cmd_tokens = list(filter(lambda a: a != '\n', shlex.split(robin_cmd)))
    ret = subprocess.run(cmd_tokens)
    assert ret.returncode == 0
