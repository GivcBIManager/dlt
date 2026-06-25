@echo off
REM One-shot setup + launcher for the OASIS control panel (Windows).
REM Delegates to setup.ps1. Usage:  setup.cmd  [-NoStart]
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0setup.ps1" %*
