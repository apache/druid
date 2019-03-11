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
import subprocess
import sys

deleted_paths_dict = {}

# assumes docs/latest in the doc repo has the current files for the next release
# deletes docs for old versions and copies docs/latest into the old versions
# run `git status | grep deleted:` on the doc repo to see what pages were deleted and feed that into
# missing-redirect-finder2.py
def main():
    if len(sys.argv) != 2:
      sys.stderr.write('usage: program <druid-docs-repo-path>\n')
      sys.exit(1)

    druid_docs_path = sys.argv[1]
    druid_docs_path = "{}/docs".format(druid_docs_path)
    prev_release_doc_paths = os.listdir(druid_docs_path)
    for doc_path in prev_release_doc_paths:
        if (doc_path != "img" and doc_path != "latest"):
            print("DOC PATH: " + doc_path)

            try:
                command = "rm -rf {}/{}/*".format(druid_docs_path, doc_path)
                outstr = subprocess.check_output(command, shell=True).decode('UTF-8')

                command = "cp -r {}/latest/* {}/{}/".format(druid_docs_path, druid_docs_path, doc_path)
                outstr = subprocess.check_output(command, shell=True).decode('UTF-8')
            except:
                print("error in path: " + doc_path)
                continue

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted, closing.')
