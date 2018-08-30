{ stdenv, batsched_dev }:

(batsched_dev.override {}).overrideAttrs (attrs: rec {
    name = "batsched-1.2.1-nix-ci";
    src = stdenv.lib.sourceByRegex ../. [
      "^src$"
      "^src/algo$"
      "^src/external$"
        ".*\.cpp$" ".*\.hpp$"
      "^cmake$"
      "^cmake/Modules$"
        ".*\.cmake"
        ".*\.cmake.in"
      "^CMakeLists\.txt$"
    ];
    enableParallelBuilding = true;
    doCheck = false;

    preConfigure = ''
      # Always start from a clean build directory
      rm -rf ./build
    '';
})
