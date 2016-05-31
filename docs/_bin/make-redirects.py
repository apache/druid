#!/usr/bin/env python

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
  target_file = os.path.join(docs_directory, normalize_target(redirect))

  # Ensure redirect source doesn't exist yet.
  if os.path.exists(source_file):
    raise Exception('Redirect source is an actual file: ' + source)

  # Ensure target *does* exist.
  if not os.path.exists(target_file) and source not in all_sources:
    raise Exception('Redirect target does not exist for source: ' + source)

  # Write redirect file
  with open(source_file, 'w') as f:
    f.write("---\n")
    f.write("layout: redirect_page\n")
    f.write("redirect_target: " + target + "\n")
    f.write("---\n")
