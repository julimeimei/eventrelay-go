param(
    [switch]$Race,
    [switch]$SkipDocker
)

$ErrorActionPreference = "Stop"

Write-Host "==> Checking Go formatting"
$goFiles = @(Get-ChildItem -Path . -Recurse -Filter *.go -File | Where-Object {
        $_.FullName -notmatch "\\vendor\\"
    } | ForEach-Object {
        $_.FullName
    })

if ($goFiles.Count -gt 0) {
    $unformatted = @(gofmt -l $goFiles)
    if ($unformatted.Count -gt 0) {
        Write-Error "Go files need formatting:`n$($unformatted -join "`n")"
    }
}

Write-Host "==> Running tests"
go test ./...

if ($Race) {
    Write-Host "==> Running race detector"
    go test -race ./...
}

Write-Host "==> Running go vet"
go vet ./...

Write-Host "==> Building binaries"
go build ./cmd/api ./cmd/worker ./cmd/demo-consumer

if (-not $SkipDocker) {
    Write-Host "==> Validating Docker Compose config"
    docker compose config | Out-Null
}

Write-Host "Quality gate passed."
