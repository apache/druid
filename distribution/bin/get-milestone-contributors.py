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

import json
import sys

import requests

# tested with python 3.6 and requests 2.13.0

if len(sys.argv) != 2:
  sys.stderr.write('usage: program <milestone-number>\n')
  sys.stderr.write('Provide the github milestone number, not name. (e.g., 19 instead of 0.10.1)\n')
  sys.exit(1)

milestone_num = sys.argv[1]

done = False
page_counter = 1
contributors = set()

# Get all users who created a closed issue or merged PR for a given milestone
while not done:
  resp = requests.get("https://api.github.com/repos/apache/druid/issues?milestone=%s&state=closed&page=%s" % (milestone_num, page_counter))

  if "Link" in resp.headers:
    pagination_link = resp.headers["Link"]

    # last page doesn't have a "next"
    if "rel=\"next\"" not in pagination_link:
      done = True
    else:
      page_counter += 1
  else:
    # Not enough issues to require pagination
    done = True

  issues = json.loads(resp.text)
  for issue in issues:
    if "pull_request" in issue:
      contributor_name = issue["user"]["login"]
      contributors.add(contributor_name)

# doesn't work as-is for python2, the contributor names are "unicode" instead of "str" in python2
contributors = sorted(contributors, key=str.lower)
for contributor_name in contributors:
  print("@%s" % contributor_name)
