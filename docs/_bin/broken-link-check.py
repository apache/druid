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
import re
import sys

#
# Checks for broken redirects (in _redirects.json) and links from markdown files to
# nonexistent pages. Does _not_ check for links to anchors that don't exist.
#

# Targets to these 'well known' pages are OK.
WELL_KNOWN_PAGES = ["/libraries.html", "/downloads.html", "/community/", "/thanks.html"]

def normalize_link(source, target):
  dirname = os.path.dirname(source)
  normalized = os.path.normpath(os.path.join(dirname, target))
  return normalized

def verify_redirects(docs_directory, redirect_json):
  ok = True

  with open(redirect_json, 'r') as f:
    redirects = json.loads(f.read())

  for redirect in redirects:
    if redirect["target"] in WELL_KNOWN_PAGES:
      continue

    # Replace .html and named anchors with .md, and check the file on the filesystem.
    target = re.sub(r'\.html(#.*)?$', '.md', normalize_link(redirect["source"], redirect["target"]))
    if not os.path.exists(os.path.join(docs_directory, target)):
      sys.stderr.write('Redirect [' + redirect["source"] + '] target does not exist: ' + redirect["target"] + "\n")
      ok = False

  return ok

def verify_markdown(docs_directory):
  ok = True

  # Get list of markdown files.
  markdowns = []
  for root, dirs, files in os.walk(docs_directory):
    for name in files:
      if name.endswith('.md'):
        markdowns.append(os.path.join(root, name))

  for markdown_file in markdowns:
    with open(markdown_file, 'r') as f:
      content = f.read()

    for m in re.finditer(r'\[([^\[]*?)\]\((.*?)(?: \"[^\"]+\")?\)', content):
      target = m.group(2)

      if target in WELL_KNOWN_PAGES:
        continue

      if markdown_file.endswith("/druid-kerberos.md") and target in ['regexp', 'druid@EXAMPLE.COM']:
        # Hack to support the fact that rule examples in druid-kerberos docs look sort of like markdown links.
        continue

      target = re.sub(r'^/docs/VERSION/', '', target)
      target = re.sub(r'#.*$', '', target)
      target = re.sub(r'\.html$', '.md', target)
      target = re.sub(r'/$', '/index.md', target)
      if target and not (target.startswith('http://') or target.startswith('https://')):
        target_normalized = normalize_link(markdown_file, target)

        if not os.path.exists(target_normalized):
          sys.stderr.write('Page     [' + markdown_file + '] target does not exist: ' + m.group(2) + "\n")
          ok = False

  return ok

def main():
  if len(sys.argv) != 3:
    sys.stderr.write('usage: program <docs dir> <redirect.json>\n')
    sys.exit(1)

  ok = verify_redirects(sys.argv[1], sys.argv[2])
  ok = verify_markdown(sys.argv[1]) and ok
  if not ok:
    sys.exit(1)

main()
