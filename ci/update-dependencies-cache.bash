#!/usr/bin/env nix-shell
#! nix-shell -i bash -p nix
set -eu

# (re)build up-to-date CI batsched package + deps, push them on binary cache
nix-build ./ci -A test_pinned | cachix push batsim
nix-build ./ci -A test_dev | cachix push batsim
