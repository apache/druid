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

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <pr_number> <repository>"
  echo "Example: $0 12345 apache/druid"
  exit 1
fi

PR_NUMBER="$1"
REPOSITORY="$2"

# Extract version from pom.xml
VERSION=$(xmllint --xpath "/*[local-name()='project']/*[local-name()='version']/text()" pom.xml | sed 's/-SNAPSHOT//')

if [ -z "$VERSION" ]; then
  echo "Error: Could not extract version from pom.xml"
  exit 1
fi

echo "Extracted version: $VERSION"

# Check if milestone exists
MILESTONE_NUMBER=$(gh api "repos/$REPOSITORY/milestones" --jq ".[] | select(.title==\"$VERSION\") | .number")

# Create milestone if it doesn't exist
if [ -z "$MILESTONE_NUMBER" ]; then
  echo "Creating milestone: $VERSION"
  MILESTONE_NUMBER=$(gh api "repos/$REPOSITORY/milestones" -f title="$VERSION" --jq '.number')
else
  echo "Milestone $VERSION already exists (number: $MILESTONE_NUMBER)"
fi

# Add PR to milestone
echo "Adding PR #$PR_NUMBER to milestone $VERSION"
gh api "repos/$REPOSITORY/issues/$PR_NUMBER" -f milestone="$MILESTONE_NUMBER" -X PATCH
echo "Successfully added PR #$PR_NUMBER to milestone $VERSION"
