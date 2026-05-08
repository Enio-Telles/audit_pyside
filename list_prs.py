import os
import urllib.request
import json

TOKEN = os.environ.get("GITHUB_TOKEN")
repo = "Eniotelles1234/audit_pyside"

url = f"https://api.github.com/repos/{repo}/pulls?state=open"
req = urllib.request.Request(url, headers={
    "Authorization": f"token {TOKEN}",
    "Accept": "application/vnd.github.v3+json",
    "User-Agent": "python"
})
try:
    with urllib.request.urlopen(req) as response:
        prs = json.loads(response.read().decode())
        for pr in prs:
            print(f"#{pr['number']}: {pr['title']} - {pr['head']['ref']}")
            comments_url = f"https://api.github.com/repos/{repo}/issues/{pr['number']}/comments"
            creq = urllib.request.Request(comments_url, headers={
                "Authorization": f"token {TOKEN}",
                "Accept": "application/vnd.github.v3+json",
                "User-Agent": "python"
            })
            with urllib.request.urlopen(creq) as cresponse:
                comments = json.loads(cresponse.read().decode())
                if comments:
                    print(f"  Comments:")
                    for c in comments:
                        print(f"  - {c['body']}")
except Exception as e:
    print(f"Error: {e}")
