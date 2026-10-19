param([int]$Port = 6379)

$ErrorActionPreference = 'Stop'
$redisVersion = '5.0.14.1'
$redisDirectory = Join-Path $env:RUNNER_TEMP "redis-$redisVersion"
$redisArchive = "$redisDirectory.zip"
$redisUrl = "https://github.com/tporadowski/redis/releases/download/v$redisVersion/Redis-x64-$redisVersion.zip"

Invoke-WebRequest -Uri $redisUrl -OutFile $redisArchive
Expand-Archive -LiteralPath $redisArchive -DestinationPath $redisDirectory -Force
$redisProcess = Start-Process -FilePath (Join-Path $redisDirectory 'redis-server.exe') `
    -ArgumentList @('--bind', '127.0.0.1', '--port', $Port, '--save', '""', '--appendonly', 'no') `
    -WorkingDirectory $redisDirectory -WindowStyle Hidden -PassThru

$redisClient = Join-Path $redisDirectory 'redis-cli.exe'
$readyAttempts = 30
for ($attempt = 0; $attempt -lt $readyAttempts; $attempt++) {
    if ($redisProcess.HasExited) {
        throw 'Redis exited before becoming ready.'
    }
    $response = & $redisClient -h 127.0.0.1 -p $Port ping 2>$null
    if ($LASTEXITCODE -eq 0 -and $response -eq 'PONG') {
        Write-Host "Redis $redisVersion is ready on port $Port."
        exit 0
    }
    Start-Sleep -Seconds 1
}
Stop-Process -Id $redisProcess.Id
throw 'Redis did not become ready within 30 seconds.'
