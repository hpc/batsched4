This directory is essentially a nix repository with some scripts.

# Packages overview
## batsched_local
This is the current version of batsched (built from current file hierarchy).

## batsim_pinned
This is the current *reference* Batsim version for batsched.
batsched should always work with this version.

## batsim_dev
This is the up-to-date Batsim version.
batsched should work with this version.

## test_deps_(pinned|dev)
The list of packages needed to run tests.
This is meant to be used as a shell, not meant to be installed.

## test_(pinned|dev)
A shell used to run tests. Essentially batsim_local + test_deps.
Not meant to be installed either.

# Useful commands
In all the following examples, the current directory is expected to be
batsched's root.

## Building packages
This can be done via `nix-build`. Result will be in `./result/`.
Some examples:
``` bash
nix-build ./ci -A batsched_local
nix-build ./ci -A batsim_pinned
```

## Install packages
This is done via `nix-env`:
``` bash
nix-env -f ./ci -iA batsched_local
nix-env -f ./ci -iA batsim_dev
```

To uninstall them, use `nix-env --uninstall`.

## Get into a shell to build packages
`nix-shell` is your friend here. Example:
``` bash
nix-shell ./ci -A batsched_local
# your shell now has all batsched's build dependencies!
# you can freely build the project (cmake, make...)
```

## Run the tests
This is essentially "run the test script in the desired environment".
``` bash
# test current batsched with batsim_pinned:
nix-shell ./ci -A test_pinned --command './ci/run-tests.bash'

# or test with batsim_dev:
nix-shell ./ci -A test_dev --command './ci/run-tests.bash'
```
