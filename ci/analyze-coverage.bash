#!/usr/bin/env nix-shell
#! nix-shell . -i bash -A test_deps_pinned
set -eu

echo "Prepare directories"
rm -rf ./cover
mkdir -p ./cover/tmp
cd ./cover/tmp

echo "Call gcov"
gcov_files=$(find ../../build -name '*.gcda')
for gcov_file in ${gcov_files[@]}; do
    gcov ${gcov_file} 1>/dev/null 2>&1
done

echo "Only keep interesting files"
interesting_sources=$(find ../../src -name '*.?pp' | sort | grep -v 'pempek_assert\|taywee_args')
set +e
for interesting_source in ${interesting_sources[@]}; do
    interesting_file="./$(basename ${interesting_source}).gcov"
    cp -f ${interesting_file} ../ 2>/dev/null
done
set -e

cd ../..
rm -rf ./cover/tmp

echo "Run gcovr analysis (human-readable report)"
gcovr -gk -o ./cover/summary.txt
cat ./cover/summary.txt

echo "Run gcovr analysis (html report)"
rm -rf ./cover/html
mkdir -p ./cover/html
gcovr -gk --html-details -o ./cover/html/index.html

exit 0
