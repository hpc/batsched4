{ stdenv, batsched, test_deps }:

stdenv.mkDerivation rec {
  name = "batsched-test-env";

  # This package is not meant to be built
  unpackPhase = "true";
  installPhase = "true";
  propagatedBuildInputs = [ batsched test_deps ];
}
