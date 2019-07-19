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
import os
import sys

def normalize_target(redirect):
  dirname = os.path.dirname(redirect["source"])
  normalized = os.path.normpath(os.path.join(dirname, redirect["target"]))
  return normalized

if len(sys.argv) != 3:
  sys.stderr.write('usage: program <docs dir> <redirect.json>\n')
  sys.exit(1)

docs_directory = sys.argv[1]
redirect_json = sys.argv[2]

with open(redirect_json, 'r') as f:
  redirects = json.loads(f.read())

all_sources = {}

# Index all redirect sources
for redirect in redirects:
  all_sources[redirect["source"]] = 1

# Create redirects
for redirect in redirects:
  source = redirect["source"]
  target = redirect["target"]
  source_file = os.path.join(docs_directory, source)

  # Ensure redirect source doesn't exist yet.
  if os.path.exists(source_file):
    raise Exception('Redirect source is an actual file: ' + source)

  # Ensure target *does* exist, if relative.
  if not target.startswith("/"):
    target_file = os.path.join(docs_directory, normalize_target(redirect))
    if not os.path.exists(target_file) and source not in all_sources:
      raise Exception('Redirect target does not exist for source: ' + source)

  # Write redirect file
  os.makedirs(os.path.dirname(source_file), exist_ok=True)
  with open(source_file, 'w') as f:
    f.write("---\n")
    f.write("layout: redirect_page\n")
    f.write("redirect_target: " + target + "\n")
    f.write("---\n")
