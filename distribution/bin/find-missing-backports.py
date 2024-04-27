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


pr_number_pattern = r'\(#(\d+)\)'
backport_pattern = r'\[Backport[^\]]*\]'

def extract_pr_title_from_commit_message(commit_msg):
    # Extract commit message except the pr number
    commit_msg = re.sub(backport_pattern, '', commit_msg)
    pr_num_pos = commit_msg.find("(#")
    if pr_num_pos < 0:
        pr_num_pos = len(commit_msg)
    return commit_msg[:pr_num_pos].strip()

def extract_pr_numbers_from_commit_message(commit_msg):
    extracted_numbers = re.findall(pr_number_pattern, commit_msg)
    return extracted_numbers

def find_missing_backports(pr_jsons, release_pr_subjects, release_pr_numbers):
    for pr in pr_jsons:
      backport_found = False
      for label in pr['labels']:
        if label['name'] == 'Backport':
          backport_found = True
      pr_title_candidate = extract_pr_title_from_commit_message(pr['title'])
      if pr_title_candidate in release_pr_subjects:
        backport_found = True
      if str(pr['number']) in release_pr_numbers:
        backport_found = True
      if backport_found == False: 
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
  sys.stderr.write("  e.g., program myusername 0.17.0 0.18.0 30")
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
release_pr_numbers = set()
for commit_msg in all_release_commits.splitlines():
    title = extract_pr_title_from_commit_message(commit_msg)
    pr_numbers = extract_pr_numbers_from_commit_message(commit_msg)
    release_pr_subjects.add(title)
    release_pr_numbers.update(pr_numbers)

print("Number of release PR subjects: {}".format(len(release_pr_subjects)))
# Get all closed PRs and filter out with milestone
milestone_url = "https://api.github.com/repos/apache/druid/milestones/{}".format(milestone_number)
resp = requests.get(milestone_url, auth=(github_username, os.environ["GIT_TOKEN"])).json()
milestone_title = resp['title']
pr_items = []
page = 0
while True:
  page = page + 1
  pr_url = "https://api.github.com/search/issues?per_page=50&page={}&q=milestone:{}+type:pr+is:merged+is:closed+repo:apache/druid".format(page,milestone_title)
  pr_resp = requests.get(pr_url, auth=(github_username, os.environ["GIT_TOKEN"])).json()
  if pr_resp['incomplete_results']:
    sys.stderr.write('This script cannot handle incomplete results')
    sys.exit(1)
  pr_items.extend(pr_resp['items'])
  if len(pr_resp['items']) < 50:
    print("Total PRs for current milestone: {}".format(len(pr_items)))
    print("Total expected count: {}".format(pr_resp['total_count']))
    if pr_resp['total_count'] != len(pr_items):
      sys.stderr.write('Expected PR count does not match with number of PRs fetched')
      sys.exit(1)
    break
find_missing_backports(pr_items, release_pr_subjects, release_pr_numbers)
