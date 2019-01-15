let
  pkgs = import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/18.03.tar.gz") {};
  kapack = import
    ( fetchTarball "https://github.com/oar-team/kapack/archive/master.tar.gz") {};
in

let
  callPackage = pkgs.lib.callPackageWith (pkgs // pkgs.xlibs // self // kapack);
  self = rec {
    inherit pkgs kapack;

    # Redefine some packages for clarity's sake
    batexpe = kapack.batexpe;
    batsim_pinned = (kapack.batsim.override {simgrid = kapack.simgrid_dev_working; }).overrideAttrs (attrs: rec {
      name = "batsim-${version}";
      version = "3.0.0-pinned";
      src = pkgs.fetchgit {
        url = "https://framagit.org/batsim/batsim.git";
        rev = "12db5085210ac24d82657b21fafe0ca198dcf48d";
        sha256 = "07b9npm5qvrzanp14rwp743dxsh7dwpvpywmlpxla5j4kxk665hc";
      };
    });
    batsim_dev = (kapack.batsim.override {simgrid = kapack.simgrid_dev_working; }).overrideAttrs (attrs: rec {
      nativeBuildInputs = attrs.nativeBuildInputs ++ [kapack.intervalset];
      name = "batsim-${version}";
      version = "3.1.0-dev";
      src = fetchTarball "https://gitlab.inria.fr/batsim/batsim/repository/master/archive.tar.gz";
    });

    pytest = pkgs.python36Packages.pytest;
    gcovr = kapack.gcovr;

    # Packages defined in this tree
    batsched_local = callPackage ./local.nix {};
    test_deps_pinned = callPackage ./test-deps.nix {
      batsim = batsim_pinned;
    };
    test_deps_dev = callPackage ./test-deps.nix {
      batsim = batsim_dev;
    };

    # Packages meant to be used as shells
    test_pinned = callPackage ./test-env.nix {
      batsched = batsched_local;
      test_deps = test_deps_pinned;
    };
    test_dev = callPackage ./test-env.nix {
      batsched = batsched_local;
      test_deps = test_deps_dev;
    };
  };
in
  self
