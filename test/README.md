### Running tests
```
make test
# or pytest, but you must prepare your env, run redis...
```

### How it works?
1. pytest generates combinations of test input (code in [conftest.py])
2. for each combination of inputs: (code in [test_runner.py])
  1. pytest generates a [robin] input file
  2. pytest generates batsim and batsched input files if needed
  3. pytest executes [robin] or [robintest] on the generated file

### Running a specific test
You can manually rerun a test with robin:
```
robin test-instances/FAILING-TEST.yaml
```

You can also run batsim and batsched in different terminals:
``` bash
# feel free to hack — e.g., prepend commands with gdb, valgrind...
./test-out/FAILING-TEST/cmd/batsim.bash
./test-out/FAILING-TEST/cmd/sched.bash
```

[conftest.py]: ./conftest.py
[test_runner.py]: ./test_runner.py
[robin]: https://framagit.org/batsim/batexpe
[robintest]: https://framagit.org/batsim/batexpe
