<#
  Stop the OASIS / HNH ETL control panel started by start-app.ps1.

  Kills the background Flask GUI process together with its whole child tree
  (which includes the embedded Dagster webserver + daemon).

    .\stop-app.ps1
#>
$ErrorActionPreference = "SilentlyContinue"
Set-Location $PSScriptRoot

$pidFile = Join-Path $PSScriptRoot "run_logs\gui-app.pid"
if (-not (Test-Path $pidFile)) {
  Write-Host "No PID file found ($pidFile). Nothing running in the background."
  exit 0
}

$procId = Get-Content $pidFile | Select-Object -First 1
if ($procId -and (Get-Process -Id $procId -ErrorAction SilentlyContinue)) {
  Write-Host "==> Stopping control panel (PID $procId) and child processes..."
  taskkill /PID $procId /T /F | Out-Null
  Write-Host "    stopped."
}
else {
  Write-Host "Process $procId is not running; cleaning up stale PID file."
}
Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
