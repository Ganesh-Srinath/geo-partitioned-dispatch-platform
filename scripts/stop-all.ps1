foreach ($port in 8080, 8081, 8082, 8083, 8084, 8085) {
    Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue |
        ForEach-Object { Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }
}
docker stop kafka | Out-Null
Write-Host "Stopped."