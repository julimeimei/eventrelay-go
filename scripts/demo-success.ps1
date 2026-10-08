$ErrorActionPreference = "Stop"

$id = [guid]::NewGuid().ToString()
$idempotencyKey = "demo-success-$id"

$body = @{
    type = "payment.approved"
    target_url = "http://demo-consumer:8081/webhook"
    payload = @{
        order_id = "ord_demo_success_$id"
        amount = 9900
    }
} | ConvertTo-Json -Depth 5

Invoke-RestMethod `
    -Method Post `
    -Uri "http://localhost:8080/webhooks" `
    -Headers @{ "Idempotency-Key" = $idempotencyKey } `
    -ContentType "application/json" `
    -Body $body
