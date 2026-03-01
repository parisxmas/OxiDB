#!/usr/bin/env bash
set -euo pipefail

KEY="$1"
DIR="$(cd "$(dirname "$0")" && pwd)"

for pkg in "$DIR"/artifacts/*.nupkg; do
    echo "Pushing $pkg ..."
    dotnet nuget push "$pkg" -k "$KEY" -s https://api.nuget.org/v3/index.json --skip-duplicate
done

echo "Done! Remember to revoke and regenerate your API key."
