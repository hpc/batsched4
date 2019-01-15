let
  pkgs = import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/18.03.tar.gz") {};
  kapack = import
    ( fetchTarball "https://github.com/oar-team/kapack/archive/master.tar.gz")
  { inherit pkgs; };
in

with kapack;
with pkgs;

(batsched_dev.override {}).overrideAttrs (attrs: rec {
    name = "batsched-1.4.0-nix-local";
    src = ../.;
    enableParallelBuilding = true;
    doCheck = false;
})
