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


def extract_pr_title_from_commit_message(commit_msg):
    # Extract commit message except the pr number
    pr_num_pos = commit_msg.find("(#")
    if pr_num_pos < 0:
        pr_num_pos = len(commit_msg)
    backport_pos = commit_msg.find("[Backport]")
    if backport_pos < 0:
        backport_pos = 0
    else:
        backport_pos = backport_pos + len("[Backport]")
    return commit_msg[backport_pos:pr_num_pos].strip()


def extract_pr_title(pr_json):
    commit_url = pr_json['commits_url']
    resp = requests.get(commit_url, auth=(github_username, os.environ["GIT_TOKEN"]))
    title_candidates = [extract_pr_title_from_commit_message(pr_json['title'])]
    if len(resp.json()) == 1:
        title_candidates.append(extract_pr_title_from_commit_message(resp.json()[0]['commit']['message']))
    return title_candidates


def find_missing_backports(pr_jsons, release_pr_subjects):
    for pr in pr_jsons:
        if pr['milestone'] is not None:
            if pr['milestone']['number'] == milestone_number:
                for pr_title_candidate in extract_pr_title(pr):
                    if pr_title_candidate in release_pr_subjects:
                        return
                print("Missing backport found for PR {}, url: {}".format(pr['number'], pr['html_url']))


def find_next_url(links):
    for link in links:
        link = link.strip()
        if link.find("rel=\"next\"") >= 0:
            match_result = re.match("<https.*>", link)
            if match_result is None:
                raise Exception("Next link[{}] is found but can't find url".format(link))
            else:
                url_holder = match_result.group(0)
                return url_holder[1:-1]
    return None


if len(sys.argv) != 5:
  sys.stderr.write('usage: program <github-username> <previous-release-branch> <current-release-branch> <milestone-number>\n')
  sys.stderr.write("  e.g., program myusername 0.14.0-incubating 0.15.0-incubating 30")
  sys.stderr.write("  It is also necessary to set a GIT_TOKEN environment variable containing a personal access token.")
  sys.exit(1)

github_username = sys.argv[1]
previous_branch = sys.argv[2]
release_branch = sys.argv[3]
milestone_number = int(sys.argv[4])

master_branch = "master"
command = "git log {}..{} --oneline | tail -1".format(master_branch, previous_branch)

# Find the commit where the previous release branch was cut from master
previous_branch_first_commit = subprocess.check_output(command, shell=True).decode('UTF-8')
match_result = re.match("(\w+) .*", previous_branch_first_commit)
if match_result is None:
    raise Exception("Can't find the first commit of the previous release.")
previous_branch_first_commit = match_result.group(1)

print("Previous branch: {}, first commit: {}".format(previous_branch, previous_branch_first_commit))

# Find all commits between that commit and the current release branch
command = "git log --pretty=tformat:%s {}..{}".format(previous_branch_first_commit, release_branch)
all_release_commits = subprocess.check_output(command, shell=True).decode('UTF-8')

release_pr_subjects = set()
for commit_msg in all_release_commits.splitlines():
    title = extract_pr_title_from_commit_message(commit_msg)
    release_pr_subjects.add(title)

# Get all closed PRs and filter out with milestone
next_url = "https://api.github.com/repos/apache/incubator-druid/pulls?state=closed"

while next_url is not None:
    resp = requests.get(next_url, auth=(github_username, os.environ["GIT_TOKEN"]))
    find_missing_backports(resp.json(), release_pr_subjects)
    links = resp.headers['Link'].split(',')
    next_url = find_next_url(links)