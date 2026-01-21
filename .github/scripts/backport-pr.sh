#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
  echo "Usage: $0 <commit_hash> <target_branch> <pr_title> [body]"
  echo "Example: $0 abc123def456 37.0.0 'Fix bug in query engine' 'Closes #12345'"
  exit 1
fi

COMMIT_HASH="$1"
TARGET_BRANCH="$2"
PR_TITLE="$3"
BODY="${4:-Automatic backport to $TARGET_BRANCH}"

BACKPORT_BRANCH="backport-$COMMIT_HASH-to-$TARGET_BRANCH"

git fetch origin "$TARGET_BRANCH"
git checkout -b "$BACKPORT_BRANCH" "origin/$TARGET_BRANCH"
git cherry-pick -x "$COMMIT_HASH"
git push origin "$BACKPORT_BRANCH"

gh pr create \
  --base "$TARGET_BRANCH" \
  --head "$BACKPORT_BRANCH" \
  --title "$PR_TITLE" \
  --body "$BODY"
