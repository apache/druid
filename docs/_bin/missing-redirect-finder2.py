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

# Takes the output of `git status | grep deleted:` on the doc repo
# and cross references deleted pages with the _redirects.json file
if len(sys.argv) != 3:
  sys.stderr.write('usage: program <del_paths_file> <redirect.json file>\n')
  sys.exit(1)

del_paths = sys.argv[1]
redirect_json_path = sys.argv[2]

dep_dict = {}
with open(del_paths, 'r') as del_paths_file:
    for line in del_paths_file.readlines():
        subidx = line.index("/", 0)
        line2 = line[subidx+1:]
        subidx = line2.index("/", 0)
        line3 = line2[subidx+1:]
        dep_dict[line3.strip("\n")] = True

existing_redirects = {}
with open(redirect_json_path, 'r') as redirect_json_file:
    redirect_json = json.load(redirect_json_file)
    for redirect_entry in redirect_json:
        redirect_source = redirect_entry["source"]
        redirect_source = redirect_source.replace(".html", ".md")
        existing_redirects[redirect_source] = True

for dep in dep_dict:
    if dep not in existing_redirects:
        print("MISSING REDIRECT: " + dep)
