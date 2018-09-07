#!/usr/bin/env nix-shell
#! nix-shell . -i bash -A test_deps_pinned
set -eu

initial_dir=$(realpath .)

# Run a redis server if needed
redis_launched_here=0
r=$(ps faux | grep redis-server | grep -v grep | wc -l)
if [ $r -eq 0 ]
then
    echo "Running a Redis server..."
    redis-server>/dev/null &
    redis_launched_here=1

    while ! nc -z localhost 6379; do
      sleep 1
    done
fi

# Add built batsched in PATH
export PATH=$(realpath ./build):${PATH}

# Set TEST_ROOT so simulation input files can be found
export TEST_ROOT=$(realpath ./test)

# Print which versions are used
echo "batsched realpath: $(realpath $(which batsched))"
echo "batsim realpath: $(realpath $(which batsim))"
echo "robin realpath: $(realpath $(which robin))"

# Execute the tests (TODO: clean tests)
cd test
pytest
failed=$?

# Stop the redis server if it has been launched by this script
if [ $redis_launched_here -eq 1 ]
then
    echo "Stopping the Redis server..."
    killall redis-server
fi

cd ${initial_dir}
exit ${failed}
