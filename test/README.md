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
You can therefore directly rerun a test (a failing one for example):
```
robin test-instances/FAILING-TEST.yaml
```

You can also run batsim and batsched in different terminals:
```
./test-out/FAILING-TEST/cmd/batsim.bash
./test-out/FAILING-TEST/cmd/batsim.bash
```

[conftest.py]: ./conftest.py
[test_runner.py]: ./test_runner.py
[robin]: https://framagit.org/batsim/batexpe
