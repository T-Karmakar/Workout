#!/usr/bin/env python3
"""
batch_update_gradle_task.py

Update a specific Gradle task in all repos under a GitLab group.
Creates a new branch + Merge Request per repo.

Requirements:
  - requests
  - gitpython

Install:
  pip install requests gitpython
"""

import os
import sys
import time
import subprocess
import re
import requests
from git import Repo

# -----------------------------
# CONFIGURATION
# -----------------------------

GITLAB_GROUP = "your-group-name"  # example: "mycompany/team1"
GITLAB_TOKEN = os.environ.get("GITLAB_TOKEN")  # store in env for security
API_URL = "https://gitlab.com/api/v4"
TMP_DIR = "/tmp/gitlab-batch-update"
TARGET_TASK_NAME = "myTask"
NEW_TASK_CONTENT = """task myTask {
    doLast {
        println 'Updated Task from Python!'
    }
}"""
BRANCH_NAME = "update-task-auto"

# -----------------------------
# HELPERS
# -----------------------------

HEADERS = {
    "PRIVATE-TOKEN": GITLAB_TOKEN
}


def get_all_project_urls():
    """Fetch all projects in the group."""
    print("[INFO] Fetching project list from GitLab API ...")
    page = 1
    repos = []

    while True:
        url = f"{API_URL}/groups/{GITLAB_GROUP}/projects?per_page=100&page={page}"
        resp = requests.get(url, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()

        if not data:
            break

        for proj in data:
            repos.append(proj["http_url_to_repo"])

        page += 1

    print(f"[INFO] Found {len(repos)} repositories.")
    return repos


def replace_task_content(build_gradle_path):
    """Replace the task in build.gradle (regex-based)."""
    with open(build_gradle_path, "r") as f:
        content = f.read()

    # Replace multiline task definition with NEW_TASK_CONTENT
    pattern = re.compile(rf"task\s+{TARGET_TASK_NAME}\s*\{{.*?\}}", re.DOTALL)
    new_content, count = pattern.subn(NEW_TASK_CONTENT, content)

    if count == 0:
        print("[WARN] No matching task found to replace.")
    else:
        with open(build_gradle_path, "w") as f:
            f.write(new_content)
        print(f"[INFO] Replaced task: {TARGET_TASK_NAME}")

    return count


def create_merge_request(project_id, branch_name):
    """Create a MR via GitLab API."""
    url = f"{API_URL}/projects/{project_id}/merge_requests"
    data = {
        "source_branch": branch_name,
        "target_branch": "main",  # change if your default branch is 'master'
        "title": "Update Gradle task automatically",
        "description": "This MR updates the specified Gradle task across repos.",
        "remove_source_branch": True
    }
    resp = requests.post(url, headers=HEADERS, data=data)
    if resp.status_code == 201:
        print(f"[INFO] Merge Request created: {resp.json().get('web_url')}")
    else:
        print(f"[ERROR] Failed to create MR: {resp.status_code} {resp.text}")


# -----------------------------
# MAIN SCRIPT
# -----------------------------

def main():
    if not GITLAB_TOKEN:
        print("[ERROR] Please set GITLAB_TOKEN as env variable.")
        sys.exit(1)

    os.makedirs(TMP_DIR, exist_ok=True)
    repos = get_all_project_urls()

    for repo_url in repos:
        repo_name = os.path.basename(repo_url).replace(".git", "")
        local_path = os.path.join(TMP_DIR, repo_name)

        print(f"\n[INFO] Processing {repo_name} ...")

        # Fresh clone
        if os.path.exists(local_path):
            subprocess.run(["rm", "-rf", local_path], check=True)

        Repo.clone_from(repo_url, local_path, branch="main", depth=1)
        repo = Repo(local_path)

        build_gradle = os.path.join(local_path, "build.gradle")
        if not os.path.isfile(build_gradle):
            print("[WARN] build.gradle not found. Skipping.")
            continue

        # Replace task
        changed = replace_task_content(build_gradle)
        if changed == 0:
            print("[INFO] No change needed. Skipping commit.")
            continue

        # Create new branch
        repo.git.checkout('-b', BRANCH_NAME)
        repo.git.add("build.gradle")
        repo.index.commit("Auto update Gradle task from Python")
        repo.git.push("--set-upstream", "origin", BRANCH_NAME)
        print(f"[INFO] Pushed branch '{BRANCH_NAME}'")

        # Get project ID to create MR
        # Fetch project info via API
        proj_info = requests.get(f"{API_URL}/projects?search={repo_name}", headers=HEADERS).json()
        proj_id = None
        for proj in proj_info:
            if proj["http_url_to_repo"] == repo_url:
                proj_id = proj["id"]
                break

        if not proj_id:
            print("[ERROR] Could not determine project ID for MR.")
            continue

        create_merge_request(proj_id, BRANCH_NAME)

    print("\n✅ [DONE] All repositories processed!")


if __name__ == "__main__":
    main()
