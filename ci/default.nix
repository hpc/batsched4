let
  pkgs = import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/18.03.tar.gz") {};
  kapack = import
    ( fetchTarball "https://github.com/oar-team/kapack/archive/master.tar.gz")
  { inherit pkgs; };
in

let
  callPackage = pkgs.lib.callPackageWith (pkgs // pkgs.xlibs // self // kapack);
  self = rec {
    inherit pkgs kapack;

    # Redefine some packages for clarity's sake
    batexpe = kapack.batexpe;
    batsim_pinned = kapack.batsim;
    batsim_dev = kapack.batsim_dev;

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
