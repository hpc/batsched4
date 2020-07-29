[![pipeline status](https://framagit.org/batsim/batsched/badges/master/pipeline.svg)](https://framagit.org/batsim/batsched/pipelines)
[![coverage](https://img.shields.io/codecov/c/github/oar-team/batsched.svg)](https://codecov.io/gh/oar-team/batsched)
[![changelog](https://img.shields.io/badge/doc-changelog-blue.svg)](./CHANGELOG.md)

**batsched** is a set of [Batsim]-compatible algorithms implemented in C++.

## Install
### For [Nix] users
``` bash
# Up-to-date version
nix-env -iA batsched-master -f 'https://github.com/oar-team/nur-kapack/archive/master.tar.gz'
# Latest release
nix-env -iA batsched -f 'https://github.com/oar-team/nur-kapack/archive/master.tar.gz'
```

### Manually
``` bash
git clone https://framagit.org/batsim/batsched.git
mkdir -p batsched/build
cd batsched/build
cmake ..
make
make install
```

Up-to-date dependencies and versions are fully defined in [batsched's CI nix recipe](./release.nix).  
Here is a quick (and probably outdated) list:
- decent clang/gcc and cmake
- zmq (C and C++)
- redox (hiredis + libev)
- [loguru]
- [intervalset]
- decent boost, gmp, rapidjson...

[Batsim]: https://framagit.org/batsim/batsim/
[intervalset]: https://framagit.org/batsim/intervalset
[loguru]: https://github.com/emilk/loguru
[Nix]: https://nixos.org/nix/
