import os
import urllib.request
import json

TOKEN = os.environ.get("GITHUB_TOKEN")
repo = "Eniotelles1234/audit_pyside"

# Let's get the PR that I just submitted on the branch jules-9437906187026254022-bb9f5c9e
url = f"https://api.github.com/repos/{repo}/pulls?head={repo.split('/')[0]}:jules-9437906187026254022-bb9f5c9e"
req = urllib.request.Request(url, headers={
    "Authorization": f"token {TOKEN}",
    "Accept": "application/vnd.github.v3+json",
    "User-Agent": "python"
})
try:
    with urllib.request.urlopen(req) as response:
        prs = json.loads(response.read().decode())
        if prs:
            pr = prs[0]
            pr_num = pr['number']
            comments_url = f"https://api.github.com/repos/{repo}/issues/{pr_num}/comments"
            creq = urllib.request.Request(comments_url, headers={
                "Authorization": f"token {TOKEN}",
                "Accept": "application/vnd.github.v3+json",
                "User-Agent": "python"
            })
            with urllib.request.urlopen(creq) as cresponse:
                comments = json.loads(cresponse.read().decode())
                print(f"Comments for PR #{pr_num}:")
                for c in comments:
                    print(f"- {c['body']}")
        else:
            print("No PR found for this branch.")
except Exception as e:
    print(f"Error: {e}")
