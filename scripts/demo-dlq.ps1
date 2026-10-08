$ErrorActionPreference = "Stop"

$id = [guid]::NewGuid().ToString()
$idempotencyKey = "demo-dlq-$id"

$body = @{
    type = "payment.rejected"
    target_url = "http://demo-consumer:8081/fail-permanent"
    payload = @{
        order_id = "ord_demo_dlq_$id"
        reason = "invalid payment method"
    }
} | ConvertTo-Json -Depth 5

Invoke-RestMethod `
    -Method Post `
    -Uri "http://localhost:8080/webhooks" `
    -Headers @{ "Idempotency-Key" = $idempotencyKey } `
    -ContentType "application/json" `
    -Body $body
