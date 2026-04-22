# Cleanup merged remote branches (PowerShell)
<#
.SYNOPSIS
  Lists remote branches merged into a base branch (default: main) and optionally deletes them.

.DESCRIPTION
  This helper fetches the remote, finds remote branches that are merged into
  the specified base (e.g. origin/main) and prints them. Use -Delete to push
  deletions to the remote, and -ConfirmEach to require confirmation per branch.

.EXAMPLE
  .\scripts\cleanup-merged-branches.ps1

.EXAMPLE
  .\scripts\cleanup-merged-branches.ps1 -Delete -ConfirmEach

#>

param(
  [switch]$Delete,
  [switch]$ConfirmEach,
  [string]$Remote = "origin",
  [string]$Main = "main"
)

Write-Host "Fetching $Remote..."
git fetch $Remote --prune 2>$null
if ($LASTEXITCODE -ne 0) {
  Write-Error "git fetch failed"
  exit 2
}

$merged = git branch -r --merged "$Remote/$Main" 2>$null |
  ForEach-Object { $_.Trim() } |
  Where-Object { $_ -and -not ($_ -match "^\s*$Remote/HEAD") -and -not ($_ -match "^\s*$Remote/$Main$") }

if (-not $merged) {
  Write-Host "No merged remote branches found (relative to $Remote/$Main)."
  exit 0
}

Write-Host "Merged remote branches (relative to $Remote/$Main):"
$merged | ForEach-Object { Write-Host " - $_" }

if ($Delete) {
  foreach ($b in $merged) {
    $branch = $b -replace "^$Remote/",""
    if ($branch -in @("main","master","develop")) {
      Write-Host "Skipping protected branch: $branch"
      continue
    }
    if ($ConfirmEach) {
      $ans = Read-Host ("Delete remote branch '{0}/{1}'? (y/N)" -f $Remote, $branch)
      if ($ans -ne 'y') {
        Write-Host "Skipping $branch"
        continue
      }
    }
    Write-Host "Deleting $Remote/$branch ..."
    git push $Remote --delete $branch
    if ($LASTEXITCODE -ne 0) {
      Write-Error "Failed to delete $Remote/$branch"
    }
  }
} else {
  Write-Host ""
  Write-Host "To delete these branches run with -Delete (use -ConfirmEach to prompt per branch)."
  Write-Host "To save the list: .\scripts\cleanup-merged-branches.ps1 > stale_branches.txt"
}
