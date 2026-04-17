#!/usr/bin/env python3
import json, subprocess, urllib.request, sys
from pathlib import Path

REPO_OWNER = "Enio-Telles"
REPO = "audit_pyside"
PR_NUMBER = 93
PR_BODY_PATH = Path(r"C:\funcoes - Copia\pr_body.md")

if not PR_BODY_PATH.exists():
    print(f"pr_body.md not found: {PR_BODY_PATH}", file=sys.stderr)
    sys.exit(2)

body = PR_BODY_PATH.read_text(encoding="utf-8")

try:
    token = subprocess.check_output(["gh", "auth", "token"]).decode().strip()
except Exception as e:
    print("failed to get gh token:", e, file=sys.stderr)
    sys.exit(3)

url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO}/pulls/{PR_NUMBER}"
headers = {
    "Authorization": f"Bearer {token}",
    "User-Agent": "update-pr-body-script",
    "Content-Type": "application/json",
}

data = json.dumps({"body": body}).encode("utf-8")
req = urllib.request.Request(url, data=data, headers=headers, method="PATCH")

try:
    with urllib.request.urlopen(req) as resp:
        resp_text = resp.read().decode("utf-8")
    print("PR updated:", url)
    print(resp_text)
except urllib.error.HTTPError as e:
    err = e.read().decode("utf-8")
    print("HTTPError", e.code, err, file=sys.stderr)
    sys.exit(4)
except Exception as e:
    print("Error", str(e), file=sys.stderr)
    sys.exit(5)
