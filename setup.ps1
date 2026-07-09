<#
  One-shot setup + launcher for the OASIS control panel (Windows / PowerShell).

    .\setup.ps1              # create/refresh .venv, install deps, start the GUI
    .\setup.ps1 -NoStart     # install only, don't launch
    $env:OASIS_GUI_PORT=9000; .\setup.ps1
#>
param([switch]$NoStart)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

$py = if ($env:PYTHON) { $env:PYTHON } else { "python" }
$venv = if ($env:VENV) { $env:VENV } else { ".venv" }

# Verify Python is available.
try { & $py --version | Out-Null } catch {
  Write-Error "'$py' not found. Install Python 3.10+ (set `$env:PYTHON to override)."
  exit 1
}

if (-not (Test-Path $venv)) {
  Write-Host "==> Creating virtual environment at $venv"
  & $py -m venv $venv
}

$vpy = Join-Path $venv "Scripts\python.exe"

Write-Host "==> Upgrading pip"
& $vpy -m pip install --upgrade pip | Out-Null

Write-Host "==> Installing dependencies (pipeline + GUI)"
& $vpy -m pip install -r requirements-gui.txt

Write-Host "==> Installing the orchestrator code location (editable)"
& $vpy -m pip install -e orchestrator

Write-Host ""
Write-Host "Setup complete. Virtualenv: $venv"
Write-Host "Reminder: Oracle 11g needs the Instant Client (thick mode) on this host —"
Write-Host "          see README.md. The GUI runs without it, but real extractions need it."
Write-Host "Reminder: ClickHouse (24.x+) is an EXTERNAL prerequisite for the dbt layer and"
Write-Host "          must be able to read the iceberg_output/ path used in icebergLocal()."
Write-Host ""

if (-not $NoStart) {
  $port = if ($env:OASIS_GUI_PORT) { $env:OASIS_GUI_PORT } else { "8765" }
  Write-Host "==> Starting OASIS control panel on http://127.0.0.1:$port"
  & $vpy gui\app.py
} else {
  Write-Host "Run the GUI later with:  $vpy gui\app.py"
}
