#!/usr/bin/env bash

set -euo pipefail

for file in samples/*.{h5,nxs,cxi}; do
    dumpfile="${file%.*}.dump.txt"
    outfile="${file%.*}.out.txt"
    echo "Dumping $file"
    cabal run hsfive -- "$file" "$dumpfile" > "$outfile" 2>&1
done
