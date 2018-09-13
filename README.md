[![pipeline status](https://framagit.org/batsim/batsched/badges/master/pipeline.svg)](https://framagit.org/batsim/batsched/pipelines)
[![coverage report](https://framagit.org/batsim/batsched/badges/master/coverage.svg)](http://batsim.gforge.inria.fr/batsched/coverage/)
[![changelog](https://img.shields.io/badge/doc-changelog-blue.svg)](./CHANGELOG.md)

**batsched** is a set of [Batsim]-compatible algorithms implemented in C++.

## Install
### For [Nix] users
``` bash
# Up-to-date version
nix-env -iA batsched_dev -f 'https://github.com/oar-team/kapack/archive/master.tar.gz'
# Latest release
nix-env -iA batsched -f 'https://github.com/oar-team/kapack/archive/master.tar.gz'
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

Up-to-date dependencies and versions are fully defined in [batsched's CI nix recipe](./default.nix).  
Here is a quick (and probably outdated) list:
- decent clang/gcc and cmake
- zmq (C and C++)
- redox (hiredis + libev)
- [loguru]
- [intervalset]
- decent boost, gmp, rapidjson, openssl...

[Batsim]: https://framagit.org/batsim/batsim/
[intervalset]: https://framagit.org/batsim/intervalset
[loguru]: https://github.com/emilk/loguru
[Nix]: https://nixos.org/nix/
