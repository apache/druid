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

# this script does some primitive examination of git diff to determine if a test suite needs to be run or not

# these jobs should always be run, no matter what
always_run_jobs = ['license checks', '(openjdk8) packaging check', '(openjdk11) packaging check']

# ignore changes to these files completely since they don't impact CI, if the changes are only to these files then all
# of CI can be skipped. however, jobs which are always run will still be run even if only these files are changed
ignore_prefixes = ['.github', '.idea', '.asf.yaml', '.backportrc.json', '.codecov.yml', '.dockerignore', '.gitignore',
                   '.lgtm.yml', 'CONTRIBUTING.md', 'setup-hooks.sh', 'upload.sh', 'dev', 'distribution/docker',
                   'distribution/asf-release-process-guide.md', '.travis.yml',
                   'owasp-dependency-check-suppressions.xml', 'licenses']

script_prefixes = ['check_test_suite.py', 'check_test_suite_test.py']
script_job = ['script checks']

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
web_console_still_run_for_java_jobs = ['web console end-to-end test']


def check_ignore(file):
    is_always_ignore = True in (file.startswith(prefix) for prefix in ignore_prefixes)
    if is_always_ignore:
        print("found ignorable file change: {}".format(file))
    return is_always_ignore

def check_testable_script(file):
    is_script = True in (file.startswith(prefix) for prefix in script_prefixes)
    if is_script:
        print("found script file change: {}".format(file))
    return is_script

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
    """
    try to determine if a test suite should run or not given a set files in a diff

    :param suite: travis job name
    :param diff_files: files changed in git diff
    :return: True if the supplied suite needs to run, False otherwise
    """

    if suite in always_run_jobs:
        # you gotta do what you gotta do
        return True

    all_ignore = True
    any_docs = False
    all_docs = True
    any_console = False
    all_console = True
    any_java = False
    any_testable_script = False
    all_testable_script = True

    # go over all of the files in the diff and collect some information about the diff contents, we'll use this later
    # to decide whether or not to run the suite
    for f in diff_files:
        is_ignore = check_ignore(f)
        all_ignore = all_ignore and is_ignore
        is_docs = check_docs(f)
        any_docs = any_docs or is_docs
        all_docs = all_docs and is_docs
        is_console = check_console(f)
        any_console = any_console or is_console
        all_console = all_console and is_console
        is_script = check_testable_script(f)
        any_testable_script = any_testable_script or is_script
        all_testable_script = all_testable_script and is_script
        any_java = any_java or (not is_ignore and not is_docs and not is_console and not is_script)

    # if everything is ignorable, we can skip this suite
    if all_ignore:
        return False
    # if the test suite is a doc job, return true if any of the files changed were docs
    if suite in docs_jobs:
        return any_docs
    # if all of the changes are docs paths, but the current suite is not a docs job, we can skip
    if all_docs:
        return False
    if suite in web_console_still_run_for_java_jobs:
        return any_console or any_java
    # if the test suite is a web console job, return true if any of the changes are web console files
    if suite in web_console_jobs:
        return any_console
    # if all of the changes are web console paths, but the current suite is not a web console job, we can skip
    if all_console:
        return False
    if suite in script_job:
        return any_testable_script
    if all_testable_script:
        return False

    # if all of the files belong to known non-java groups, we can also skip java
    # note that this should probably be reworked to much more selectively run the java jobs depending on the diff
    if not any_java:
        return False

    # we don't know we can skip for sure, so lets run it
    return True


def failWithUsage():
    sys.stderr.write("usage: check_test_suite.py <test-suite-name>\n")
    sys.stderr.write("  e.g., check_test_suite.py docs")
    sys.exit(1)


if __name__ == '__main__':
    suite_name = ""

    # when run by travis, we run this script without arguments, so collect the test suite name from environment
    # variables. if it doesn't exist, fail
    if len(sys.argv) == 1:
        if 'TRAVIS_JOB_NAME' in os.environ:
            suite_name = os.environ['TRAVIS_JOB_NAME']
        else:
            failWithUsage()
    elif len(sys.argv) == 2:
        # to help with testing, can explicitly pass a test suite name
        suite_name = sys.argv[1]
    else:
        failWithUsage()

    # we only selectively run CI for PR builds, branch builds such as master and releases will always run all suites
    is_pr = False
    if 'TRAVIS_PULL_REQUEST' in os.environ and os.environ['TRAVIS_PULL_REQUEST'] != 'false':
        is_pr = True

    if not is_pr:
        print("Not a pull request build, need to run all test suites")
        sys.exit(1)

    # this looks like it only gets the last commit, but the way travis PR builds work this actually gets the complete
    # diff (since all commits from the PR are rebased onto the target branch and added as a single commit)
    all_changed_files_string = subprocess.check_output("git diff --name-only HEAD~1", shell=True).decode('UTF-8')
    all_changed_files = all_changed_files_string.splitlines()
    print("Checking if suite '{}' needs to run test on diff:\n{}".format(suite_name, all_changed_files_string))

    # we should always run all test suites for builds that are not for a pull request
    needs_run = check_should_run_suite(suite_name, all_changed_files)

    if needs_run:
        print("Changes detected, need to run test suite '{}'".format(suite_name))
        sys.exit(1)

    print("No applicable changes detected, can skip test suite '{}'".format(suite_name))
    sys.exit(0)
