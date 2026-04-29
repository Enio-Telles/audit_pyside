# Branch cleanup: reproducible process

This repository previously included a static file `stale_branches.txt` listing remote branches proposed for deletion. Tracking a static list in the repo is brittle and can lead to accidental loss or stale entries. Instead, this project uses a small reproducible script to discover merged remote branches and (optionally) delete them.

Files added:

- `scripts/cleanup-merged-branches.ps1` — PowerShell helper that fetches the remote, lists branches merged into `origin/main`, and can delete them when invoked with `-Delete`.

Usage (Windows / PowerShell):

```powershell
# list merged remote branches (no deletion)
.\scripts\cleanup-merged-branches.ps1 -Remote origin -Main main

# interactively delete merged branches
.\scripts\cleanup-merged-branches.ps1 -Delete -ConfirmEach

# non-interactive deletion (use with caution)
.\scripts\cleanup-merged-branches.ps1 -Delete
```

Recommendations

- Review the list printed by the script before running with `-Delete`.
- The script skips a small set of protected branch names (`main`, `master`, `develop`). Adjust the script if your team uses different protected branches.
- For automated cleanup, prefer running the script from CI or a local operator account after manual approval rather than committing static lists.
