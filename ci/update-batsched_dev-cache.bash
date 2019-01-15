#!/usr/bin/env nix-shell
#! nix-shell -i bash -p nix
set -eu

# Build up-to-date batsched_dev package, push it on binary cache
nix-build ${KAPACK:-https://github.com/oar-team/kapack/archive/master.tar.gz} -A batsched_dev | cachix push batsim
