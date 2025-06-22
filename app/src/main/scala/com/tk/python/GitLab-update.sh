#!/usr/bin/env bash

set -euo pipefail

## -----------------------------
## CONFIGURATION
## -----------------------------
GITLAB_GROUP="your-group-name"
GITLAB_TOKEN="YOUR_GITLAB_PERSONAL_ACCESS_TOKEN"
API_URL="https://gitlab.com/api/v4"
TMP_DIR="/tmp/gitlab-batch-update"
NEW_TASK_CONTENT=$(cat <<'EOF'
task myTask {
    doLast {
        println 'Updated Task!'
    }
}
EOF
)

# Target pattern to replace in build.gradle
TARGET_TASK_NAME="myTask"

## -----------------------------
## 1️⃣ Get list of project HTTPS clone URLs
## -----------------------------
echo "[INFO] Fetching project list from GitLab..."

mkdir -p "$TMP_DIR"

curl --header "PRIVATE-TOKEN: $GITLAB_TOKEN" \
  --silent \
  "$API_URL/groups/$GITLAB_GROUP/projects?per_page=100" \
  | grep -o '"http_url_to_repo":"[^"]*' | sed 's/"http_url_to_repo":"//' > "$TMP_DIR/repos.txt"

echo "[INFO] Found $(wc -l < "$TMP_DIR/repos.txt") repositories."

## -----------------------------
## 2️⃣ Loop over each repository
## -----------------------------
while read -r REPO_URL; do
  REPO_NAME=$(basename "$REPO_URL" .git)
  echo "[INFO] Processing $REPO_NAME ..."

  cd "$TMP_DIR"
  rm -rf "$REPO_NAME"
  git clone --depth 1 "$REPO_URL" "$REPO_NAME"

  cd "$REPO_NAME"

  ## -----------------------------
  ## 3️⃣ Replace the task in build.gradle
  ## -----------------------------
  if [ -f "build.gradle" ]; then
    echo "[INFO] Updating build.gradle in $REPO_NAME ..."

    # Use perl to replace whole task definition
    perl -0777 -i -pe "s/task\s+$TARGET_TASK_NAME\s*\{[^\}]*\}/$NEW_TASK_CONTENT/s" build.gradle

    ## Optional: verify
    grep "$TARGET_TASK_NAME" build.gradle || echo "[WARN] Task not found after replace"

    ## -----------------------------
    ## 4️⃣ Commit and push back
    ## -----------------------------
    git config user.name "AutoUpdater"
    git config user.email "ci-bot@example.com"
    git checkout -b update-task

    git add build.gradle
    git commit -m "Update $TARGET_TASK_NAME task automatically"

    git push origin update-task

    echo "[INFO] Pushed branch 'update-task' for $REPO_NAME"

  else
    echo "[WARN] build.gradle not found in $REPO_NAME, skipping."
  fi

done < "$TMP_DIR/repos.txt"

echo "[DONE] All repositories processed. Review PRs!"
