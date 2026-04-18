Suggested trivial fixes for PR #100

What I created:

- `0001-fix-ambiguous-loop-vars.patch`: unified-diff that adds `scripts/check_parens.py` with the ambiguous loop variable fixed (`l` -> `line`).
- `scripts_check_parens_fixed.py`: the fixed file content for quick review.

How to apply the patch to the PR branch (example):

```bash
# fetch the PR branch (if you have remote access)
git fetch origin refs/pull/100/head:pr100
git checkout -b fix/pr100-trivial pr100

# apply the patch (from repo root)
git apply pr100_fixes/0001-fix-ambiguous-loop-vars.patch
# or copy the fixed file into scripts/check_parens.py
mv pr100_fixes/scripts_check_parens_fixed.py scripts/check_parens.py

git add scripts/check_parens.py
git commit -m "fix(scripts): rename ambiguous loop variable l -> line (lint: E741)"
# push and open PR from this branch
git push origin fix/pr100-trivial
```

Notes:
- These are small, non-invasive fixes intended as a first suggested commit for PR #100.
- I can apply more trivial fixes (line-length breaks, small refactors) if you want me to prepare additional patches.
