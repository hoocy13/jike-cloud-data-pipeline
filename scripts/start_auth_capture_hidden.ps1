param(
    [int]$Port = 18765
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$logDir = Join-Path $repoRoot 'logs'
$stdoutLog = Join-Path $logDir 'auth_capture_assistant.log'
$stderrLog = Join-Path $logDir 'auth_capture_assistant.error.log'

$listener = Get-NetTCPConnection -LocalAddress '127.0.0.1' -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
if ($listener) {
    exit 0
}

New-Item -ItemType Directory -Path $logDir -Force | Out-Null
$pythonLauncher = Get-Command 'py.exe' -ErrorAction Stop
$arguments = @('-3', 'scripts\auth_capture_assistant.py')

Start-Process `
    -FilePath $pythonLauncher.Source `
    -ArgumentList $arguments `
    -WorkingDirectory $repoRoot `
    -WindowStyle Hidden `
    -RedirectStandardOutput $stdoutLog `
    -RedirectStandardError $stderrLog

$deadline = (Get-Date).AddSeconds(10)
do {
    Start-Sleep -Milliseconds 250
    $listener = Get-NetTCPConnection -LocalAddress '127.0.0.1' -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
} until ($listener -or (Get-Date) -ge $deadline)

if (-not $listener) {
    throw "Capture assistant did not listen on 127.0.0.1:$Port within 10 seconds. See $stderrLog"
}
