#!/usr/bin/env python3
import os
import os.path
import subprocess
from collections import namedtuple

class RobinInstance(object):
    def __init__(self, output_dir, batcmd, schedcmd, simulation_timeout, ready_timeout, success_timeout, failure_timeout):
        self.output_dir = output_dir
        self.batcmd = batcmd
        self.schedcmd = schedcmd
        self.simulation_timeout = simulation_timeout
        self.ready_timeout = ready_timeout
        self.success_timeout = success_timeout
        self.failure_timeout = failure_timeout

    def to_yaml(self):
        # Manual dump to avoid dependencies
        return f'''output-dir: '{self.output_dir}'
batcmd: "{self.batcmd}"
schedcmd: "{self.schedcmd}"
simulation-timeout: {self.simulation_timeout}
ready-timeout: {self.ready_timeout}
success-timeout: {self.success_timeout}
failure-timeout: {self.failure_timeout}
'''

    def to_file(self, filename):
        # Create parent directory if needed
        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))

        # Write the file
        robin_file = open(filename, "w")
        robin_file.write(self.to_yaml())
        robin_file.close()

def gen_batsim_cmd(platform, workload, output_dir, more_flags):
    return f"batsim -p '{platform}' -w '{workload}' -e '{output_dir}' {more_flags}"

def run_robin(filename):
    return subprocess.run(['robin', filename])

