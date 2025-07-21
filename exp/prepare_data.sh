#!/bin/bash
set -e pipefail

# git clone http://www.aiops.cn/gitlab/aiops-live-benchmark/aiopschallengedata2025.git
# mv aiopschallengedata2025/* data/

# find "data/" -type f -name "*.tar.gz" | while read -r file; do
#     echo "decompressing: $file"
    
#     dir=$(dirname "$file")
    
#     if ! tar -xzf "$file" -C "$dir"; then
#         echo "Failed to decompress: $file"
#     else
#         echo "Successfully decompressed: $file"
#         rm "$file"
#     fi
# done

# git clone http://www.aiops.cn/gitlab/aiops-live-benchmark/phaseone.git

find "phasetwo/" -type f -name "*.tar.gz" | while read -r file; do
    echo "decompressing: $file"
    
    dir=$(dirname "$file")
    
    if ! tar -xzf "$file" -C "$dir"; then
        echo "Failed to decompress: $file"
    else
        echo "Successfully decompressed: $file"
        rm "$file"
    fi
done

echo "Decompression completed."