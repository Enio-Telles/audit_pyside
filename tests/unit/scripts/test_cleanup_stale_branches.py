"""Unit tests for scripts/cleanup_stale_branches.py."""

from __future__ import annotations

import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))

import cleanup_stale_branches as csb


def test_protected_branches_includes_main_master_develop() -> None:
    assert "main" in csb.PROTECTED
    assert "master" in csb.PROTECTED
    assert "develop" in csb.PROTECTED


def test_skips_branch_when_merge_base_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """Branch is excluded from results when git merge-base fails for that branch."""
    call_count = 0

    def fake_run(cmd: list[str], **kwargs: object) -> MagicMock:
        nonlocal call_count
        call_count += 1
        result = MagicMock()
        result.stdout = ""
        result.returncode = 0

        joined = " ".join(str(c) for c in cmd)
        if "branch" in joined and "-r" in joined and "--no-color" in joined:
            if "--merged" in joined:
                result.stdout = ""
            else:
                result.stdout = "  origin/stale-feature\n  origin/main\n"
        elif "merge-base" in joined:
            raise subprocess.CalledProcessError(128, cmd)
        elif "log" in joined:
            result.stdout = "2026-01-01T00:00:00+00:00"
        return result

    monkeypatch.setattr(subprocess, "run", fake_run)

    branches = csb.collect_branches(
        remote="origin",
        base="main",
        merged_only=False,
        min_age_days=0,
    )

    short_names = [b.short_name for b in branches]
    assert "stale-feature" not in short_names, (
        "Branch should be skipped when merge-base fails"
    )


def test_dry_run_does_not_invoke_delete(monkeypatch: pytest.MonkeyPatch) -> None:
    """print_dry_run must never invoke git push --delete."""
    deleted_calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: object) -> MagicMock:
        if "push" in cmd and "--delete" in cmd:
            deleted_calls.append(cmd)
        result = MagicMock()
        result.stdout = ""
        result.returncode = 0
        return result

    monkeypatch.setattr(subprocess, "run", fake_run)

    branches = [
        csb.BranchInfo(
            name="origin/old-feature",
            short_name="old-feature",
            last_commit_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            merged=True,
        )
    ]

    csb.print_dry_run(branches, remote="origin")

    assert deleted_calls == [], (
        "print_dry_run must not invoke any git push --delete commands"
    )
