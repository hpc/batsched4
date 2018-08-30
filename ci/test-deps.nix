{ stdenv, batsim, batexpe,
  which, redis, procps, psmisc,
  nix-prefetch-git
}:

stdenv.mkDerivation rec {
  name = "batsched-test-deps";

  # This package is not meant to be built
  unpackPhase = "true";
  installPhase = "true";
  propagatedBuildInputs = [ batsim batexpe
    which redis procps psmisc
    nix-prefetch-git
  ];
}
