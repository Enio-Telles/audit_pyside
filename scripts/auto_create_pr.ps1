<#
Auto-create a PR using gh CLI if authenticated.
Usage:
  .\auto_create_pr.ps1 -Branch fix/frontend-datatable-eslint-migration -Title "My PR title" -BodyFile .\pr_body.md -Base main
#>
param(
  [string]$Branch = $(git rev-parse --abbrev-ref HEAD),
  [string]$Title = "",
  [string]$BodyFile = ".\pr_body.md",
  [string]$Base = "main"
)

if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
  Write-Error "gh CLI not found. Install GitHub CLI and authenticate with 'gh auth login'."
  exit 2
}

# Check auth
$auth = gh auth status 2>&1
if ($LASTEXITCODE -ne 0) {
  Write-Host "gh not authenticated. Run 'gh auth login' and re-run this script.";
  Write-Host $auth;
  exit 1
}

if (-not (Test-Path $BodyFile)) {
  Write-Warning "Body file $BodyFile not found; creating a minimal body.";
  "$Title`n`nAutomated PR created by agent." | Out-File -Encoding utf8 $BodyFile
}

if ([string]::IsNullOrWhiteSpace($Title)) { $Title = "Automated PR: $Branch" }

Write-Host "Creating PR from $Branch into $Base..."
gh pr create --title $Title --body-file $BodyFile --base $Base --head $Branch

if ($LASTEXITCODE -eq 0) {
  Write-Host "PR created successfully."
} else {
  Write-Error "Failed to create PR. Exit code: $LASTEXITCODE"
}
