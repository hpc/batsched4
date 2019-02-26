let
  kapack = import
    ( fetchTarball "https://github.com/oar-team/kapack/archive/master.tar.gz")
  { };
in

with kapack;

(batsched_dev.override {}).overrideAttrs (attrs: rec {
    name = "batsched-1.4.0-nix-local";
    src = ../.;
    enableParallelBuilding = true;
    doCheck = false;
    dontStrip = true;
})
