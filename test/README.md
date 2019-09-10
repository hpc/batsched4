### Running tests
``` bash
nix-shell ../release.nix -A integration_tests --command 'pytest'
# or just pytest, but you must prepare your env...
```

Optionally, use Batsim's binary cache to avoid recompiling many things (e.g., SimGrid).

``` bash
nix-env -iA cachix -f https://cachix.org/api/v1/install # installs cachix
cachix use batsim # add Batsim's cachix storage as a Nix remote cache
```

### How does it work?
0. nix-shell puts you into an environment where batsched, batsim, robin, redis, etc. are available (code in [release.nix])
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
# feel free to hack these files — e.g., prepend commands with gdb, valgrind...
./test-out/FAILING-TEST/cmd/batsim.bash
./test-out/FAILING-TEST/cmd/sched.bash
```

[release.nix]: ../release.nix
[conftest.py]: ./conftest.py
[test_runner.py]: ./test_runner.py
[robin]: https://framagit.org/batsim/batexpe
[robintest]: https://framagit.org/batsim/batexpe
