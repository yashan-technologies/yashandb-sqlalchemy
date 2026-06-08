# Run YashanDB project-owned regression tests (PowerShell-safe; no glob needed).
# If you see "禁止运行脚本", use instead:
#   scripts\run_project_tests.cmd
#   python scripts\run_project_tests.py --dburi "..."
# Or: powershell -ExecutionPolicy Bypass -File .\scripts\run_project_tests.ps1
param(
    [string]$DbUri = $env:YASHANDB_URL
)

if (-not $DbUri) {
    $DbUri = "yashandb+yaspy://MY_TEST001:123456@172.16.90.87:1688/test"
}

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$testFiles = @(
    "test\project_test_sqlalchemy20_compat.py",
    "test\project_test_compile.py",
    "test\project_test_reflection.py",
    "test\project_test_types.py",
    "test\project_test_returning.py",
    "test\project_test_orm_smoke.py"
)

python -m pytest @testFiles --dburi $DbUri @args
