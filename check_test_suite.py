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

# do some primitive examination of git diff to determine if a test suite needs to be run or not

always_run_jobs = ['license checks', '(openjdk8) packaging check', '(openjdk11) packaging check']

# ignore changes to these files completely since they don't impact CI, if the changes are only to these files then all
# of CI can be skipped
ignore_prefixes = ['.github', '.idea', '.asf.yaml', '.backportrc.json', '.codecov.yml', '.dockerignore', '.gitignore',
                   '.lgtm.yml', 'CONTRIBUTING.md', 'setup-hooks.sh', 'upload.sh', 'dev', 'distribution/docker',
                   'distribution/asf-release-process-guide.md', '.travis.yml', 'check_test_suite.py']
# these files are docs changes
# if changes are limited to this set then we can skip web-console and java
# if no changes in this set we can skip docs
docs_prefixes = ['docs/', 'website/']
# travis docs job name
docs_jobs = ['docs']
# these files are web-console changes
# if changes are limited to this set then we can skip docs and java
# if no changes in this set we can skip web-console
web_console_prefixes = ['web-console/']
# travis web-console job name
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


# given a test suite name (travis job name) and set of files in a git diff
# try to determine if the test suite should run or not
def check_should_run_suite(suite, diff_files):
    if suite in always_run_jobs:
        # you gotta do what you gotta do
        return True

    # is everything in the ignorable list
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

def failWithUsage():
    sys.stderr.write("usage: check_test_suite.py <test-suite-name>\n")
    sys.stderr.write("  e.g., check_test_suite.py docs")
    sys.exit(1)

if __name__ == '__main__':
    suite_name = ""

    if len(sys.argv) == 1:
        if 'TRAVIS_JOB_NAME' in os.environ:
            suite_name = os.environ['TRAVIS_JOB_NAME']
        else:
            failWithUsage()
    elif len(sys.argv) == 2:
        suite_name = sys.argv[1]
    else:
        failWithUsage()


    all_changed_files_string = subprocess.check_output("git diff --name-only HEAD~1", shell=True).decode('UTF-8')
    all_changed_files = all_changed_files_string.splitlines()
    print("Checking if suite '{}' needs to run test on diff:\n{}".format(suite_name, all_changed_files_string))

    # we should always run all test suites for builds that are not for a pull request
    is_pr = False
    if 'TRAVIS_PULL_REQUEST' in os.environ and os.environ['TRAVIS_PULL_REQUEST'] != 'false':
        is_pr = True
    needs_run = not is_pr or check_should_run_suite(suite_name, all_changed_files)

    if needs_run:
        print("Changes detected, need to run test suite '{}'".format(suite_name))
        sys.exit(1)

    print("No applicable changes detected, can skip test suite '{}'".format(suite_name))
    sys.exit(0)

