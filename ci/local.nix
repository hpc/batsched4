{ batsched_dev }:

(batsched_dev.override {}).overrideAttrs (attrs: rec {
    name = "batsched-1.2.1-nix-ci";
    src = ../.;
    enableParallelBuilding = true;
    doCheck = false;

    preConfigure = ''
      # Always start from a clean build directory
      rm -rf ./build
    '';
})
