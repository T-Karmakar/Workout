#!/usr/bin/env bash
#
# update_gradle_task.sh
#
# For each repo in a GitLab group:
# - Find main branch + latest branch
# - Replace a task in build.gradle
# - Commit & push directly
# - Uses only Bash + Git + curl + perl
#
# Requirements:
#   - Git Bash (Windows) OR any Linux/macOS shell
#   - curl, git, perl
#

set -euo pipefail

########################################
# CONFIGURATION
########################################

GITLAB_GROUP="your-group-name"
GITLAB_TOKEN="YOUR_GITLAB_PERSONAL_ACCESS_TOKEN"
API_URL="https://gitlab.com/api/v4"
WORK_DIR="/tmp/gitlab-task-update"
TARGET_TASK_NAME="myTask"

# Replacement block (use <<'EOF' for literal)
read -r -d '' NEW_TASK_CONTENT <<'EOF'
task myTask {
    doLast {
        println 'Updated Task from Bash!'
    }
}
EOF

########################################
# UTILS
########################################

auth_header="PRIVATE-TOKEN: $GITLAB_TOKEN"

mkdir -p "$WORK_DIR"

########################################
# 1️⃣ GET ALL REPOS
########################################

echo "[INFO] Fetching all projects in group: $GITLAB_GROUP"

# handle pagination if needed
page=1
repos=()

while : ; do
    resp=$(curl -s --header "$auth_header" "$API_URL/groups/$GITLAB_GROUP/projects?per_page=100&page=$page")
    urls=$(echo "$resp" | grep -o '"http_url_to_repo":"[^"]*' | cut -d'"' -f4)
    if [ -z "$urls" ]; then break; fi
    repos+=($urls)
    ((page++))
done

echo "[INFO] Found ${#repos[@]} repos."

########################################
# 2️⃣ PROCESS EACH REPO
########################################

for repo_url in "${repos[@]}"; do

  repo_name=$(basename "$repo_url" .git)
  echo "----------------------------------------"
  echo "[INFO] Processing: $repo_name"

  # Fetch default branch
  project_encoded=$(echo "$repo_url" | sed 's#https://gitlab.com/##' | sed 's#/#%2F#g')
  project_info=$(curl -s --header "$auth_header" "$API_URL/projects/$project_encoded")

  default_branch=$(echo "$project_info" | grep -o '"default_branch":"[^"]*' | cut -d'"' -f4)
  [ -z "$default_branch" ] && default_branch="main"

  # Get latest updated branch name too
  latest_branch=$(curl -s --header "$auth_header" "$API_URL/projects/$project_encoded/repository/branches?order_by=updated_at" | grep -o '"name":"[^"]*' | head -n1 | cut -d'"' -f4)
  [ -z "$latest_branch" ] && latest_branch="$default_branch"

  echo "[INFO] Default branch: $default_branch, Latest branch: $latest_branch"

  # Prepare local folder
  cd "$WORK_DIR"
  rm -rf "$repo_name"
  mkdir -p "$repo_name"

  # Clone default branch
  echo "[INFO] Cloning default branch..."
  git clone --depth 1 --branch "$default_branch" "$repo_url" "$repo_name/$default_branch"

  # Clone latest branch if different
  if [[ "$latest_branch" != "$default_branch" ]]; then
    echo "[INFO] Cloning latest branch..."
    git clone --depth 1 --branch "$latest_branch" "$repo_url" "$repo_name/$latest_branch"
  fi

  ########################################
  # 3️⃣ UPDATE build.gradle + COMMIT
  ########################################

  for BRANCH in "$default_branch" "$latest_branch"; do
    branch_dir="$repo_name/$BRANCH"
    [ ! -d "$branch_dir" ] && continue

    echo "[INFO] Updating build.gradle for branch: $BRANCH"

    cd "$branch_dir"

    if [ -f "build.gradle" ]; then
      # Replace task using Perl (multiline safe)
      perl -0777 -i -pe "s/task\s+$TARGET_TASK_NAME\s*\{[^\}]*\}/$NEW_TASK_CONTENT/s" build.gradle

      # Commit + push
      git config user.name "AutoBot"
      git config user.email "ci-bot@example.com"

      git add build.gradle
      if git diff --cached --quiet; then
        echo "[INFO] No change for $BRANCH. Skipping commit."
      else
        git commit -m "Update $TARGET_TASK_NAME task via script"
        git push origin "$BRANCH"
        echo "[INFO] Pushed update to $BRANCH."
      fi

    else
      echo "[WARN] build.gradle not found in $BRANCH, skipping."
    fi

    cd "$WORK_DIR"
  done

done

echo "[✅ DONE] All repositories processed."
