#!/usr/bin/env python3

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

import os
import re
import requests
import subprocess
import sys
import time

if len(sys.argv) != 5:
  sys.stderr.write('usage: program <github-username> <upstream-remote> <previous-release-branch> <current-release-branch>\n')
  sys.stderr.write("  e.g., program myusername upstream 0.17.0 0.18.0")
  sys.stderr.write("  It is also necessary to set a GIT_TOKEN environment variable containing a personal access token.")
  sys.exit(1)

github_username = sys.argv[1]
upstream_remote = sys.argv[2]
previous_branch = sys.argv[3]
release_branch = sys.argv[4]
master_branch = "master"

upstream_master = "{}/{}".format(upstream_remote, master_branch)
upstream_previous = "{}/{}".format(upstream_remote, previous_branch)
upstream_release = "{}/{}".format(upstream_remote, release_branch)

command = "git log {}..{} --oneline | tail -1".format(upstream_master, upstream_previous)

# Find the commit where the previous release branch was cut from master
previous_branch_first_commit = subprocess.check_output(command, shell=True).decode('UTF-8')
match_result = re.match("(\w+) .*", previous_branch_first_commit)
previous_branch_first_commit = match_result.group(1)

print("Previous branch: {}, first commit: {}".format(upstream_previous, previous_branch_first_commit))

# Find all commits between that commit and the current release branch
command = "git rev-list {}..{}".format(previous_branch_first_commit, upstream_release)
all_release_commits = subprocess.check_output(command, shell=True).decode('UTF-8')

for commit_id in all_release_commits.splitlines():
    try:
        # wait 3 seconds between calls to avoid hitting the rate limit
        time.sleep(3)

        search_url = "https://api.github.com/search/issues?q=type:pr+is:merged+is:closed+repo:apache/druid+SHA:{}"
        resp = requests.get(search_url.format(commit_id), auth=(github_username, os.environ["GIT_TOKEN"]))
        resp_json = resp.json()

        milestone_found = False
        closed_pr_nums = []
        if (resp_json.get("items") is None):
            print("Could not get PRs for commit ID {}, resp: {}".format(commit_id, resp_json))
            continue

        for pr in resp_json["items"]:
            closed_pr_nums.append(pr["number"])
            milestone = pr["milestone"]
            if milestone is not None:
                milestone_found = True
                print("COMMIT: {},  PR#: {},  MILESTONE: {}".format(commit_id, pr["number"], pr["milestone"]["url"]))
        if not milestone_found:
            print("NO MILESTONE FOUND FOR COMMIT: {}, CLOSED PRs: {}".format(commit_id, closed_pr_nums))

    except Exception as e:
        print("Got exception for commitID: {}  ex: {}".format(commit_id, e))
        continue
