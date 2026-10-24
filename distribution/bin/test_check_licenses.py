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

import importlib.util
import os
import unittest

spec = importlib.util.spec_from_file_location(
    "check_licenses", os.path.join(os.path.dirname(os.path.abspath(__file__)), "check-licenses.py")
)
check_licenses = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check_licenses)

APACHE = "Apache License version 2.0"
AZURE = "extensions/druid-azure-extensions"
TESTCONTAINERS = "extensions/druid-testcontainers"
RANGER = "extensions-contrib/druid-ranger-security"
EXCLUDED = ["extensions-contrib/", TESTCONTAINERS]
GAV = check_licenses.get_dep_key("org.example", "a", "1.0")


def find_missing(reporting_modules, registered_modules, skipping=None):
    reported = {GAV: (APACHE, reporting_modules[0])}
    registered = {GAV: set(registered_modules)} if registered_modules else {}
    return check_licenses.find_missing_licenses(reported, {GAV: set(reporting_modules)}, registered, skipping or {}, EXCLUDED)


class FindMissingLicensesTest(unittest.TestCase):
    def test_registered_under_released_module(self):
        self.assertEqual([], find_missing([AZURE], ["java-core"]))

    def test_unregistered_is_missing(self):
        self.assertEqual([(AZURE,) + GAV + (APACHE,)], find_missing([AZURE], []))

    def test_registered_only_under_excluded_module_is_missing_when_shipped(self):
        self.assertEqual([(AZURE,) + GAV + (APACHE,)], find_missing([TESTCONTAINERS, AZURE], [RANGER]))

    def test_registered_only_under_excluded_module_is_fine_when_not_shipped(self):
        self.assertEqual([], find_missing([TESTCONTAINERS], [TESTCONTAINERS]))

    def test_skipped_dependency_is_not_missing(self):
        self.assertEqual([], find_missing([AZURE], [], skipping={GAV: {}}))


class FindDruidModuleNameTest(unittest.TestCase):
    def test_maps_report_dirs_to_module_tags(self):
        self.assertEqual("java-core", check_licenses.find_druid_module_name("target/license-reports/core/reports"))
        self.assertEqual(AZURE, check_licenses.find_druid_module_name("target/license-reports/ext/druid-azure-extensions/reports"))


if __name__ == "__main__":
    unittest.main()
