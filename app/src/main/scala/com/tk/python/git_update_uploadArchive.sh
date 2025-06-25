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
  git clone --depth 1 --branch "$default_branch" "$repo_url" "$repo_name/$default_branch"

  # Clone latest branch if different
  if [[ "$latest_branch" != "$default_branch" ]]; then
    echo "[INFO] Cloning $latest_branch..."
    git clone --depth 1 --branch "$latest_branch" "$repo_url" "$repo_name/$latest_branch"
  fi

  #########################################################
  ## ✅ Replace uploadArchives in both branches
  #########################################################

  for BRANCH in "$default_branch" "$latest_branch"; do
    branch_dir="$repo_name/$BRANCH"
    [ ! -d "$branch_dir" ] && continue

    echo "[INFO] Processing branch: $BRANCH"
    cd "$branch_dir"

    if [ -f "build.gradle" ]; then

      # Write replacement to temp file to safely pass to Perl
      echo "$REPLACEMENT_BLOCK" > ../replacement_block.txt

      # Perl replace: robust nested brace matcher
      perl -0777 -i -pe '
        my $replacement = do { local $/; open my $fh, "<", "../replacement_block.txt"; <$fh> };
        while (/uploadArchives\s*\{/gc) {
          my $start = $-[0];
          my $pos = pos();
          my $depth = 1;
          while ($depth && $pos < length()) {
            my $c = substr($_, $pos, 1);
            $depth++ if $c eq "{";
            $depth-- if $c eq "}";
            $pos++;
          }
          substr($_, $start, $pos - $start) = $replacement;
        }
      ' build.gradle

      # Commit + push
      git config user.name "AutoBot"
      git config user.email "ci-bot@example.com"

      git add build.gradle

      if git diff --cached --quiet; then
        echo "[INFO] No changes for $BRANCH — skip commit."
      else
        git commit -m "Auto-update $TARGET_BLOCK_NAME block"
        git push origin "$BRANCH"
        echo "[INFO] ✅ Pushed update to $BRANCH."
      fi

    else
      echo "[WARN] build.gradle not found in $BRANCH — skip."
    fi

    cd "$WORK_DIR"
  done

done

echo ""
echo "✅✅✅ [DONE] All repositories processed successfully!"


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


###################################################################

get_latest_develop_branch_by_commit_pure() {
  local project_id="$1"
  local token="$2"
  local api_url="https://gitlab.com/api/v4"
  local auth_header="PRIVATE-TOKEN: $token"

  echo "[INFO] Finding latest 'develop_' branch by last commit date (pure Bash)..."

  # Step 1: Get ALL branch names
  branches=$(curl -s --header "$auth_header" \
    "$api_url/projects/$project_id/repository/branches" \
    | grep -o '"name":"[^"]*' | cut -d'"' -f4 | grep '^develop_')

  latest_branch=""
  latest_commit_date="1970-01-01T00:00:00Z"

  for branch in $branches; do
    # Get the single branch info
    branch_info=$(curl -s --header "$auth_header" \
      "$api_url/projects/$project_id/repository/branches/$branch")

    # Extract commit date (look for "committed_date":"...")
    commit_date=$(echo "$branch_info" | grep -o '"committed_date":"[^"]*' | cut -d'"' -f4)

    # Compare ISO 8601 timestamps lexically: safe!
    if [[ "$commit_date" > "$latest_commit_date" ]]; then
      latest_commit_date="$commit_date"
      latest_branch="$branch"
    fi
  done

  echo "$latest_branch"
}


####################################################################################

my $replacement = do {
    local $/;
    open my $fh, "<", "../replacement_block.txt" or die "Can't open file: $!";
    <$fh>
};
