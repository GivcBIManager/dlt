<#
  Start the OASIS / HNH ETL control panel (Flask GUI + embedded Dagster).

    .\start-app.ps1                     # development mode, foreground
    .\start-app.ps1 -Environment prod   # production mode, background (detached)
    .\start-app.ps1 -Environment dev  -Background
    .\start-app.ps1 -Environment prod -Foreground

  Environments (presets, only applied when not already set in the environment):
    dev   -> host 127.0.0.1, debug on,  binds to localhost only
    prod  -> host 0.0.0.0,   debug off, listens on every interface

  Override any setting with env vars before launching:
    OASIS_GUI_HOST, OASIS_GUI_PORT (8765), OASIS_GUI_DEBUG,
    OASIS_GUI_USER / OASIS_GUI_PASSWORD (required for any non-loopback bind;
      prompted for interactively when missing),
    OASIS_ALLOW_CUSTOM_CMD (1 to permit the free-form 'custom' run script),
    OASIS_DAGSTER_AUTOSTART (1), OASIS_DAGSTER_PORT (3000)
#>
param(
  [ValidateSet("dev", "prod")]
  [string]$Environment = "dev",
  [switch]$Background,
  [switch]$Foreground
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

$vpy = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $vpy)) {
  Write-Error "Virtualenv not found at $vpy. Run .\setup.ps1 first."
  exit 1
}

# Environment presets -- only set when the caller has not overridden them.
if ($Environment -eq "prod") {
  if (-not $env:OASIS_GUI_HOST)  { $env:OASIS_GUI_HOST = "0.0.0.0" }
  if (-not $env:OASIS_GUI_DEBUG) { $env:OASIS_GUI_DEBUG = "0" }
  # The panel can launch processes and edit config; a public interface needs
  # login credentials (the app enforces this too). Prompt for any that are
  # missing when a terminal is attached; otherwise fail closed.
  $publicBind = $env:OASIS_GUI_HOST -ne "127.0.0.1" -and $env:OASIS_GUI_HOST -ne "::1"
  if ($publicBind -and (-not $env:OASIS_GUI_USER -or -not $env:OASIS_GUI_PASSWORD)) {
    if (-not [Console]::IsInputRedirected) {
      Write-Host "==> Login credentials required for public bind on $($env:OASIS_GUI_HOST)"
      if (-not $env:OASIS_GUI_USER) {
        $env:OASIS_GUI_USER = Read-Host "    GUI username"
      }
      if (-not $env:OASIS_GUI_PASSWORD) {
        $secret = Read-Host "    GUI password" -AsSecureString
        $env:OASIS_GUI_PASSWORD = [System.Net.NetworkCredential]::new('', $secret).Password
      }
      if (-not $env:OASIS_GUI_USER -or -not $env:OASIS_GUI_PASSWORD) {
        Write-Error "Username and password must be non-empty."
        exit 1
      }
    }
    else {
      Write-Error @"
Refusing to start prod on $($env:OASIS_GUI_HOST) without authentication.
Set login credentials first, e.g.:
  `$env:OASIS_GUI_USER = 'admin'; `$env:OASIS_GUI_PASSWORD = '<secret>'
Then sign in at  http://<host>:$($env:OASIS_GUI_PORT)/login
(or bind to 127.0.0.1 behind a reverse proxy that handles auth).
"@
      exit 1
    }
  }
}
else {
  if (-not $env:OASIS_GUI_HOST)  { $env:OASIS_GUI_HOST = "127.0.0.1" }
  if (-not $env:OASIS_GUI_DEBUG) { $env:OASIS_GUI_DEBUG = "1" }
}
if (-not $env:OASIS_GUI_PORT) { $env:OASIS_GUI_PORT = "8765" }

$runDir = Join-Path $PSScriptRoot "run_logs"
New-Item -ItemType Directory -Force -Path $runDir | Out-Null
$pidFile = Join-Path $runDir "gui-app.pid"
$outLog  = Join-Path $PSScriptRoot "gui-server.log"
$errLog  = Join-Path $PSScriptRoot "gui-server.err.log"

# Refuse to start a second instance on top of a live one.
if (Test-Path $pidFile) {
  $old = Get-Content $pidFile -ErrorAction SilentlyContinue | Select-Object -First 1
  if ($old -and (Get-Process -Id $old -ErrorAction SilentlyContinue)) {
    Write-Error "App already running (PID $old). Stop it first with .\stop-app.ps1."
    exit 1
  }
  Remove-Item $pidFile -ErrorAction SilentlyContinue
}

# prod defaults to background, dev to foreground -- explicit switches win.
if ($Background)      { $runBackground = $true }
elseif ($Foreground) { $runBackground = $false }
else                 { $runBackground = ($Environment -eq "prod") }

Write-Host "==> Starting control panel [$Environment] on http://$($env:OASIS_GUI_HOST):$($env:OASIS_GUI_PORT)"

if ($runBackground) {
  $proc = Start-Process -FilePath $vpy -ArgumentList "gui\app.py" `
    -WorkingDirectory $PSScriptRoot -PassThru -WindowStyle Hidden `
    -RedirectStandardOutput $outLog -RedirectStandardError $errLog
  $proc.Id | Out-File -FilePath $pidFile -Encoding ascii
  Write-Host "    running in background (PID $($proc.Id)); logs -> $outLog"
  Write-Host "    stop with:  .\stop-app.ps1"
}
else {
  Write-Host "    running in foreground (Ctrl+C to stop)"
  & $vpy gui\app.py
}
