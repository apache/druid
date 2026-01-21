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

get_milestone_number() {
  local milestone_title="$1"
  local number=$(gh api "repos/$REPOSITORY/milestones" --jq ".[] | select(.title==\"$milestone_title\") | .number")

  if [ -z "$number" ]; then
    echo "Creating milestone: $milestone_title"
    number=$(gh api "repos/$REPOSITORY/milestones" -f title="$milestone_title" --jq '.number')
  fi

  echo "$number"
}

create_backport_issue() {
  local pr_number="$1"
  local pr_title="$2"
  local target_milestone="$3"

  local milestone_number=$(get_milestone_number "$target_milestone")
  local message="Backport #$pr_number to release branch for version $target_milestone"
  local note="Add the \`approved\` label to start the automatic backport process."
  local body=$(jq -n --arg source_pr "$pr_number" --arg target_version "$target_milestone" --arg message "$message" --arg note "$note" '$ARGS.named')

  gh api "repos/$REPOSITORY/issues" \
    -f title="[$target_milestone] $pr_title" \
    -f body="$body" \
    -f milestone="$milestone_number" \
    -f labels[]="backport" \
    --jq '.number'
}

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <pr_number> <repository>"
  echo "Example: $0 12345 apache/druid"
  exit 1
fi

PR_NUMBER="$1"
REPOSITORY="$2"

VERSION=$(xmllint --xpath "/*[local-name()='project']/*[local-name()='version']/text()" pom.xml | sed 's/-SNAPSHOT//')

if [ -z "$VERSION" ]; then
  echo "Error: Could not extract version from pom.xml"
  exit 1
fi

echo "Extracted version: $VERSION"

EXISTING_MILESTONE=$(gh api "repos/$REPOSITORY/issues/$PR_NUMBER" --jq '.milestone.title // empty')

if [ -n "$EXISTING_MILESTONE" ] && [ "$EXISTING_MILESTONE" != "$VERSION" ]; then
  echo "PR #$PR_NUMBER has milestone $EXISTING_MILESTONE, but should be $VERSION"

  PR_TITLE=$(gh api "repos/$REPOSITORY/issues/$PR_NUMBER" --jq '.title')
  BACKPORT_ISSUE=$(create_backport_issue "$PR_NUMBER" "$PR_TITLE" "$EXISTING_MILESTONE")

  echo "Created backport issue #$BACKPORT_ISSUE for milestone $EXISTING_MILESTONE"
elif [ -n "$EXISTING_MILESTONE" ]; then
  echo "PR #$PR_NUMBER already has correct milestone: $EXISTING_MILESTONE"
  exit 0
fi

MILESTONE_NUMBER=$(get_milestone_number "$VERSION")

echo "Adding PR #$PR_NUMBER to milestone $VERSION"
gh api "repos/$REPOSITORY/issues/$PR_NUMBER" -f milestone="$MILESTONE_NUMBER" -X PATCH
echo "Successfully added PR #$PR_NUMBER to milestone $VERSION"
