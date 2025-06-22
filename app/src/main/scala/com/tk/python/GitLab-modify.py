#!/usr/bin/env python3

"""
update_gradle_task_inplace.py

✅ For all repos in a GitLab group:
  - Identify default branch and latest branch
  - Directly update a task in build.gradle
  - Commit & push in place (no new branch, no MR)

Requirements:
  pip install requests gitpython
"""

import os
import sys
import subprocess
import re
import requests
from git import Repo

# ---------------------------------------
# CONFIG
# ---------------------------------------

GITLAB_GROUP = "your-group-name"
GITLAB_TOKEN = os.environ.get("GITLAB_TOKEN")
API_URL = "https://gitlab.com/api/v4"
TMP_DIR = "/tmp/gitlab-batch-inplace"
TARGET_TASK_NAME = "myTask"

NEW_TASK_CONTENT = """task myTask {
    doLast {
        println '✅ Updated task in place!'
    }
}"""

HEADERS = {"PRIVATE-TOKEN": GITLAB_TOKEN}

# ---------------------------------------
# HELPERS
# ---------------------------------------

def get_all_project_info():
    """Fetch all projects in the group."""
    print("[INFO] Fetching project list ...")
    page = 1
    all_projects = []

    while True:
        url = f"{API_URL}/groups/{GITLAB_GROUP}/projects?per_page=100&page={page}"
        resp = requests.get(url, headers=HEADERS)
        resp.raise_for_status()
        data = resp.json()

        if not data:
            break

        all_projects.extend(data)
        page += 1

    print(f"[INFO] Found {len(all_projects)} repositories.")
    return all_projects


def get_latest_branch(project_id):
    """Get latest branch by most recent commit."""
    url = f"{API_URL}/projects/{project_id}/repository/branches?per_page=100"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    branches = resp.json()
    if not branches:
        return None

    # Pick branch with most recent commit date
    branches.sort(key=lambda b: b["commit"]["committed_date"], reverse=True)
    return branches[0]["name"]


def replace_task(build_gradle_path):
    """Replace the target task using regex."""
    with open(build_gradle_path, "r") as f:
        content = f.read()

    pattern = re.compile(rf"task\s+{TARGET_TASK_NAME}\s*\{{.*?\}}", re.DOTALL)
    new_content, count = pattern.subn(NEW_TASK_CONTENT, content)

    if count > 0:
        with open(build_gradle_path, "w") as f:
            f.write(new_content)
        print(f"[INFO] Task '{TARGET_TASK_NAME}' replaced.")
    else:
        print(f"[WARN] Task '{TARGET_TASK_NAME}' not found.")
    return count


# ---------------------------------------
# MAIN
# ---------------------------------------

def main():
    if not GITLAB_TOKEN:
        print("[ERROR] Please export GITLAB_TOKEN.")
        sys.exit(1)

    os.makedirs(TMP_DIR, exist_ok=True)
    projects = get_all_project_info()

    for proj in projects:
        repo_url = proj["http_url_to_repo"]
        default_branch = proj["default_branch"]
        project_id = proj["id"]
        repo_name = proj["name"]

        latest_branch = get_latest_branch(project_id)
        branches_to_update = set(filter(None, [default_branch, latest_branch]))

        local_repo_path = os.path.join(TMP_DIR, repo_name)
        if os.path.exists(local_repo_path):
            subprocess.run(["rm", "-rf", local_repo_path], check=True)

        print(f"\n[INFO] Cloning {repo_name} ...")
        Repo.clone_from(repo_url, local_repo_path)
        repo = Repo(local_repo_path)

        for branch in branches_to_update:
            print(f"[INFO] Checking out branch '{branch}' ...")
            repo.git.checkout(branch)

            build_gradle = os.path.join(local_repo_path, "build.gradle")
            if not os.path.isfile(build_gradle):
                print(f"[WARN] build.gradle not found in {repo_name} on {branch}")
                continue

            if replace_task(build_gradle):
                repo.git.add("build.gradle")
                repo.index.commit(f"Auto update {TARGET_TASK_NAME} task (in place)")
                repo.git.push("origin", branch)
                print(f"[INFO] Pushed update to '{branch}' in {repo_name}")
            else:
                print(f"[INFO] No ch
