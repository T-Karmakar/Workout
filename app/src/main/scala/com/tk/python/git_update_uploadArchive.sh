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

############################################
#!/usr/bin/env bash

GITLAB_TOKEN="YOUR_PERSONAL_ACCESS_TOKEN"
GITLAB_GROUP="your-group-path"  # e.g., company/team
API_URL="https://gitlab.com/api/v4"

auth_header="PRIVATE-TOKEN: $GITLAB_TOKEN"

# 1️⃣ Get project list from group
page=1
while : ; do
  projects=$(curl -s --header "$auth_header" "$API_URL/groups/$GITLAB_GROUP/projects?per_page=100&page=$page")
  project_ids=$(echo "$projects" | grep -o '"id":[0-9]*' | cut -d':' -f2)
  [ -z "$project_ids" ] && break

  for project_id in $project_ids; do
    echo ""
    echo "🔍 Project ID: $project_id"

    # 2️⃣ Get default branch
    default_branch=$(curl -s --header "$auth_header" "$API_URL/projects/$project_id" \
      | grep -o '"default_branch":"[^"]*' | cut -d'"' -f4)
    echo "✅ Default branch: $default_branch"

    # 3️⃣ Get branches sorted by update time and filter by name
    latest_develop=$(curl -s --header "$auth_header" \
      "$API_URL/projects/$project_id/repository/branches?order_by=updated_at&sort=desc" \
      | grep -o '"name":"develop_[^"]*' | cut -d'"' -f4 | head -n1)

    if [ -n "$latest_develop" ]; then
      echo "✅ Latest 'develop_' branch: $latest_develop"
    else
      echo "❌ No 'develop_' branch found"
    fi

  done

  ((page++))
done
