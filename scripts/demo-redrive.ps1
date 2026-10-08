param(
    [Parameter(Mandatory = $true)]
    [string]$EventID
)

$ErrorActionPreference = "Stop"

Invoke-RestMethod `
    -Method Post `
    -Uri "http://localhost:8080/webhooks/$EventID/redrive"
