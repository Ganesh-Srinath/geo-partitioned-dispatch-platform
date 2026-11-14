param([switch]$Build)

# ---- Edit these three paths if yours differ ----
$Root    = Split-Path $PSScriptRoot -Parent
$Zone    = Join-Path $Root "dispatch-platform"
$Rides   = Join-Path $Root "ride-service"
$Gateway = Join-Path $Root "gateway"
# -------------------------------------------------

function Find-Jar($dir) {
    $jar = Get-ChildItem "$dir\target\*.jar" -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -notlike "*plain*" } | Select-Object -First 1
    if ($jar) { return $jar.FullName }
    return $null
}

function Ensure-Jar($dir) {
    if ($Build -or -not (Find-Jar $dir)) {
        Write-Host "Building $dir ..."
        Push-Location $dir
        .\mvnw.cmd clean package -DskipTests -q
        Pop-Location
    }
    $jar = Find-Jar $dir
    if (-not $jar) { Write-Host "No jar found in $dir\target, build failed?"; exit 1 }
    return $jar
}

function Launch($title, $dir, $jar, $extra) {
    $cmd = "`$Host.UI.RawUI.WindowTitle = '$title'; Set-Location '$dir'; java -jar '$jar' $extra"
    Start-Process powershell -ArgumentList "-NoExit", "-Command", $cmd
}

function Wait-Port($port) {
    Write-Host "Waiting for port $port " -NoNewline
    for ($i = 0; $i -lt 90; $i++) {
        try {
            $c = New-Object System.Net.Sockets.TcpClient
            $c.Connect("localhost", $port)
            $c.Close()
            Write-Host " up"
            return
        } catch {
            Start-Sleep -Seconds 2
            Write-Host "." -NoNewline
        }
    }
    Write-Host " TIMED OUT"
}

# 0. Docker must be running
docker info *> $null
if ($LASTEXITCODE -ne 0) { Write-Host "Start Docker Desktop first."; exit 1 }

# 1. Build the jars (first time, or when you pass -Build)
$zoneJar = Ensure-Jar $Zone
$ridesJar = Ensure-Jar $Rides
$gatewayJar = Ensure-Jar $Gateway

# 2. Kafka
if (docker ps -a --format "{{.Names}}" | Select-String -Pattern "^kafka$") {
    docker start kafka | Out-Null
} else {
    docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.0 | Out-Null
}
Wait-Port 9092

# 3. The four zone servers
Launch "south-primary 8081" $Zone $zoneJar "--server.port=8081"
Launch "north-primary 8082" $Zone $zoneJar "--server.port=8082"
Launch "south-replica 8083" $Zone $zoneJar "--server.port=8083 --replica.of=http://localhost:8081"
Launch "north-replica 8084" $Zone $zoneJar "--server.port=8084 --replica.of=http://localhost:8082"
foreach ($p in 8081, 8082, 8083, 8084) { Wait-Port $p }

# 4. ride-service, then the gateway
Launch "ride-service 8085" $Rides $ridesJar ""
Wait-Port 8085
Launch "gateway 8080" $Gateway $gatewayJar ""
Wait-Port 8080

Write-Host "Everything is up. Opening the dashboard..."
Start-Process "http://localhost:8085/"