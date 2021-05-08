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

# ignore completely - .idea, anything in distribution except pom,
# java - ignore docs, website, web-console only changes, otherwise run
# docs - diff in docs/ or website/
# web-console - diff in web-console/

ignore_prefixes = ['.idea', 'distribution/bin', 'distribution/docker', 'distribution/asf-release-process-guide.md']
docs_prefixes = ['docs/', 'website/']
web_console_prefixes = ['web-console/']
docs_jobs = ['docs']
web_console_jobs = ['web console', 'web console end-to-end test']


def check_ignore(file):
    is_always_ignore = True in (file.startswith(prefix) for prefix in ignore_prefixes)
    if is_always_ignore:
        print("found ignorable file change: {}".format(file))
    return is_always_ignore

def check_docs(file):
    is_docs = True in (file.startswith(prefix) for prefix in docs_prefixes)
    if is_docs:
        print("found docs file change: {}".format(file))
    return is_docs

def check_console(file):
    is_console = True in (file.startswith(prefix) for prefix in web_console_prefixes)
    if is_console:
        print("found web-console file change: {}".format(file))
    return is_console

def check_should_run_suite(suite, diff_files):
    all_ignore = True
    any_docs = False
    all_docs = True
    any_console = False
    all_console = True

    for f in diff_files:
        all_ignore = all_ignore and check_ignore(f)
        is_docs = check_docs(f)
        any_docs = any_docs or is_docs
        all_docs = all_docs and is_docs
        is_console = check_console(f)
        any_console = any_console or is_console
        all_console = any_console and is_console

    if all_ignore:
        return False
    if suite in docs_jobs:
        return any_docs
    if all_docs:
        return False
    if suite in web_console_jobs:
        return any_console
    if all_console:
        return False

    return True

suite_name = ""

if len(sys.argv) != 2:
    if 'TRAVIS_JOB_NAME' in os.environ:
        suite_name = os.environ['TRAVIS_JOB_NAME']
    else:
        sys.stderr.write("usage: check-test-suite.py <test-suite-name>\n")
        sys.stderr.write("  e.g., check-test-suite.py docs")
        sys.exit(1)
else:
    suite_name = sys.argv[1]


all_changed_files_string = subprocess.check_output("git diff --name-only HEAD~1", shell=True).decode('UTF-8')
all_changed_files = all_changed_files_string.splitlines()

print("Checking if suite '{}' needs to run test on diff:\n{}".format(suite_name, all_changed_files_string))

needs_run = check_should_run_suite(suite_name, all_changed_files)
if needs_run:
    print("Changes detected, need to run test suite '{}'".format(suite_name))
    sys.exit(1)

print("No applicable changes detected, can skip test suite '{}'".format(suite_name))
sys.exit(0)

