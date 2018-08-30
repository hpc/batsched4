#!/usr/bin/env nix-shell
#! nix-shell . -i bash -A test_deps_dev

if [ "$#" -ne 1 ]; then
    echo 'usage: pin-batsim.bash BATSIM-REV'
    exit 1
fi

rev=$1
nix-prefetch-git \
    --url https://framagit.org/batsim/batsim.git \
    --rev ${rev} \
    > batsim-pinned.json
