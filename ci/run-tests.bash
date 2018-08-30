#!/usr/bin/env nix-shell
#! nix-shell . -i bash -A test_dev
set -eu

# Run a redis server if needed
# redis_launched_here=0
# r=$(ps faux | grep redis-server | grep -v grep | wc -l)
# if [ $r -eq 0 ]
# then
#     echo "Running a Redis server..."
#     redis-server>/dev/null &
#     redis_launched_here=1

#     while ! nc -z localhost 6379; do
#       sleep 1
#     done
# fi

# Add built batsched in PATH
export PATH=$(realpath ./build):${PATH}

# Set TEST_ROOT so simulation input files can be found
export TEST_ROOT=$(realpath ./test)

# Print which versions are used
echo "batsched realpath: $(realpath $(which batsched))"
echo "batsim realpath: $(realpath $(which batsim))"
echo "robin realpath: $(realpath $(which robin))"

# Execute the tests (TODO: clean tests)
set +e
find ./test/instances -name '*.yaml' | \
    sed -E 's/(.*)/robintest \1 --test-timeout 30 --expect-robin-success --expect-sched-success --expect-batsim-success/' | \
    bash -x
failed=0


# Stop the redis server if it has been launched by this script
# if [ $redis_launched_here -eq 1 ]
# then
#     echo "Stopping the Redis server..."
#     killall redis-server
# fi

exit ${failed}
