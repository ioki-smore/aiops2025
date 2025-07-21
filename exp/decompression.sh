#!/bin/bash
set -eo pipefail

directories=("data/" "phaseone/" "phasetwo/")

process_tar() {
    local file="$1"
    echo "Decompressing: $file"
    
    local dir=$(dirname "$file")
    
    if tar -xzf "$file" -C "$dir"; then
        echo "Successfully decompressed: $file"
        rm "$file"
    else
        echo "Failed to decompress: $file" >&2
        return 1
    fi
}

for dir in "${directories[@]}"; do
    find "$dir" -type f -name "*.tar.gz" | while read -r file; do
        process_tar "$file"
    done
done

echo "Decompression completed."