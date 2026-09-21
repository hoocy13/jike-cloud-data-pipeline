param(
    [switch]$DryRun,
    [switch]$Uninstall
)

$ErrorActionPreference = 'Stop'
$taskName = 'JikeAuthCaptureAssistant'
$repoRoot = Split-Path -Parent $PSScriptRoot
$launcher = Join-Path $PSScriptRoot 'start_auth_capture_hidden.ps1'
$logDir = Join-Path $repoRoot 'logs'
$currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$powershell = (Get-Command 'powershell.exe' -ErrorAction Stop).Source
$actionArguments = "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$launcher`""

if ($DryRun) {
    Write-Output "TaskName=$taskName"
    Write-Output "User=$currentUser"
    Write-Output "WorkingDirectory=$repoRoot"
    Write-Output "Launcher=$launcher"
    Write-Output "Logs=$logDir"
    exit 0
}

if ($Uninstall) {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
    Write-Output "[DONE] Removed scheduled task: $taskName"
    exit 0
}

$action = New-ScheduledTaskAction -Execute $powershell -Argument $actionArguments -WorkingDirectory $repoRoot
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $currentUser
$principal = New-ScheduledTaskPrincipal -UserId $currentUser -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable -ExecutionTimeLimit ([TimeSpan]::Zero)
$task = New-ScheduledTask -Action $action -Trigger $trigger -Principal $principal -Settings $settings `
    -Description 'Start the local Jike/BSCM authentication capture assistant after Windows logon.'

Register-ScheduledTask -TaskName $taskName -InputObject $task -Force | Out-Null
Write-Output "[DONE] Registered scheduled task: $taskName"
Write-Output "[INFO] Logs: $logDir"
