#!/usr/bin/env bash
#
# batch_update_uploadArchives.sh
#
# ✅ For each GitLab project in a group:
#   - Finds default branch & latest branch
#   - Clones shallowly
#   - Replaces full uploadArchives block (nested braces handled)
#   - Commits & pushes directly (in-place update)
#
# ⚡️ Uses: curl + git + perl (no Python needed)
#

set -euo pipefail

#########################################################
## ✅ CONFIGURATION
#########################################################

GITLAB_GROUP="your-group-name"              # e.g. "mycompany/backend"
GITLAB_TOKEN="YOUR_GITLAB_PERSONAL_TOKEN"   # use export or fill here
API_URL="https://gitlab.com/api/v4"
WORK_DIR="/tmp/gitlab-uploadArchives-update"
BRANCH_NAME_SUFFIX="auto-uploadArchives-update"
TARGET_BLOCK_NAME="uploadArchives"

# Your replacement block (multi-line!)
read -r -d '' REPLACEMENT_BLOCK <<'EOF'
uploadArchives {
    repositories {
        maven {
            url "s3://my-bucket/releases"
        }
    }
}
EOF

#########################################################
## ✅ Fetch all project URLs in the group (pagination)
#########################################################

echo "[INFO] Fetching all repositories under group: $GITLAB_GROUP"

page=1
repos=()

while : ; do
  resp=$(curl -s --header "PRIVATE-TOKEN: $GITLAB_TOKEN" \
    "$API_URL/groups/$GITLAB_GROUP/projects?per_page=100&page=$page")

  urls=$(echo "$resp" | grep -o '"http_url_to_repo":"[^"]*' | cut -d'"' -f4)

  [ -z "$urls" ] && break

  repos+=($urls)
  ((page++))
done

echo "[INFO] Found ${#repos[@]} repositories to process."
mkdir -p "$WORK_DIR"

#########################################################
## ✅ Process each repo: default & latest branch
#########################################################

for repo_url in "${repos[@]}"; do
  repo_name=$(basename "$repo_url" .git)
  echo ""
  echo "=============================================="
  echo "[INFO] Processing: $repo_name"

  # Get default branch
  project_path=$(echo "$repo_url" | sed 's#https://gitlab.com/##' | sed 's#/#%2F#g')
  project_info=$(curl -s --header "PRIVATE-TOKEN: $GITLAB_TOKEN" "$API_URL/projects/$project_path")
  default_branch=$(echo "$project_info" | grep -o '"default_branch":"[^"]*' | cut -d'"' -f4)
  [ -z "$default_branch" ] && default_branch="main"

  # Get latest branch by updated_at
  latest_branch=$(curl -s --header "PRIVATE-TOKEN: $GITLAB_TOKEN" \
    "$API_URL/projects/$project_path/repository/branches?order_by=updated_at" \
    | grep -o '"name":"[^"]*' | head -n1 | cut -d'"' -f4)

  [ -z "$latest_branch" ] && latest_branch="$default_branch"

  echo "[INFO] Default branch: $default_branch | Latest branch: $latest_branch"

  # Prepare local clone folders
  cd "$WORK_DIR"
  rm -rf "$repo_name"
  mkdir -p "$repo_name"

  # Clone default branch
  echo "[INFO] Cloning $default_branch..."
  git clone --depth 1 --branch "$defaul
