param([int]$Drivers = 40, [int]$Rides = 15)

$gw = "http://localhost:8080"
$inv = [cultureinfo]::InvariantCulture

function Send-Post($path, $body) {
    Invoke-RestMethod -Uri "$gw$path" -Method Post -ContentType "application/json" -Body $body | Out-Null
}

Write-Host "Registering $Drivers drivers..."
1..$Drivers | ForEach-Object {
    $lat = (12.80 + (Get-Random -Minimum 0 -Maximum 4000) / 10000).ToString($inv)
    $lng = (77.50 + (Get-Random -Minimum 0 -Maximum 2000) / 10000).ToString($inv)
    Send-Post "/drivers/location" ('{"id":"v' + $_ + '","lat":' + $lat + ',"lng":' + $lng + ',"free":true}')
}

Write-Host "Requesting $Rides rides..."
1..$Rides | ForEach-Object {
    $lat = (12.80 + (Get-Random -Minimum 0 -Maximum 4000) / 10000).ToString($inv)
    $lng = (77.50 + (Get-Random -Minimum 0 -Maximum 2000) / 10000).ToString($inv)
    Send-Post "/rides/request" ('{"riderId":"rider' + $_ + '","lat":' + $lat + ',"lng":' + $lng + '}')
    Start-Sleep -Milliseconds 400
}

Write-Host "One ride whose payment fails (watch the red events)..."
Send-Post "/rides/request" '{"riderId":"nopay-demo","rideId":"demo-nopay","lat":12.93,"lng":77.62}'

Write-Host "Done. Dashboard: http://localhost:8085/"