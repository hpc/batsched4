### Running tests
```
pytest
```

### How it works?
1. pytest generates combinations of test input (code in [conftest.py][conftest.py])
2. for each combination of inputs: (code in [test_runner.py][test_runner.py])
  1. pytest generates a [robin][robin] input file
  2. pytest executes [robin][robin] on the generated file

### Running a specific test
You can manually rerun a test with [robin][robin] (a failing one for example):
```
robin test-instances/FAILING-TEST.yaml
```

You can also run batsim and batsched in different terminals:
``` bash
./test-out/FAILING-TEST/cmd/batsim.bash

# feel free to hack — e.g., prepend command with gdb, valgrind...
./test-out/FAILING-TEST/cmd/sched.bash
```

[conftest.py]: ./conftest.py
[test_runner.py]: ./test_runner.py
[robin]: https://framagit.org/batsim/batexpe
