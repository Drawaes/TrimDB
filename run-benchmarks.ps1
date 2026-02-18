param(
    [string]$Filter = "*",
    [switch]$Disasm,
    [switch]$NoRebuild
)

$ErrorActionPreference = "Stop"
$repoRoot = $PSScriptRoot
$project = Join-Path $repoRoot "benchmarks\TrimDB.Benchmarks"
$artifacts = Join-Path $repoRoot "BenchmarkDotNet.Artifacts"

if (-not $NoRebuild) {
    Write-Host "Building benchmarks in Release..." -ForegroundColor Cyan
    dotnet build $project -c Release --nologo
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Build failed." -ForegroundColor Red
        exit 1
    }
}

$bdnArgs = @("--filter", $Filter, "--artifacts", $artifacts)

if ($Disasm) {
    $bdnArgs += "--disasm"
}

Write-Host "Running benchmarks with filter: $Filter" -ForegroundColor Cyan
dotnet run --project $project -c Release --no-build -- @bdnArgs

Write-Host ""
Write-Host "Artifacts written to: $artifacts" -ForegroundColor Green
