#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
PROJECT="$REPO_ROOT/benchmarks/TrimDB.Benchmarks"
ARTIFACTS="$REPO_ROOT/BenchmarkDotNet.Artifacts"

FILTER="*"
DISASM=""
NO_REBUILD=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --filter)
            FILTER="$2"
            shift 2
            ;;
        --disasm)
            DISASM="--disasm"
            shift
            ;;
        --no-rebuild)
            NO_REBUILD=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--filter PATTERN] [--disasm] [--no-rebuild]"
            exit 1
            ;;
    esac
done

if [ "$NO_REBUILD" = false ]; then
    echo "Building benchmarks in Release..."
    dotnet build "$PROJECT" -c Release --nologo
fi

BDN_ARGS=(--filter "$FILTER" --artifacts "$ARTIFACTS")

if [ -n "$DISASM" ]; then
    BDN_ARGS+=(--disasm)
fi

echo "Running benchmarks with filter: $FILTER"
dotnet run --project "$PROJECT" -c Release --no-build -- "${BDN_ARGS[@]}"

echo ""
echo "Artifacts written to: $ARTIFACTS"
