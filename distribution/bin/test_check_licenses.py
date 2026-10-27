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
    "refresh_license_versions", os.path.join(os.path.dirname(os.path.abspath(__file__)), "refresh-license-versions.py")
)
refresh_license_versions = importlib.util.module_from_spec(spec)
spec.loader.exec_module(refresh_license_versions)
check_licenses = refresh_license_versions.check_licenses

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


def entry(name, module, version, libraries, extra=""):
    return (
        "name: {}\nlicense_category: binary\nmodule: {}\nlicense_name: Apache License version 2.0\n"
        "version: {}\nlibraries:\n{}{}"
    ).format(name, module, version, "".join("  - {}\n".format(library) for library in libraries), extra)


def licenses_yaml(*entries):
    return "# header\n\n---\n\n" + "\n---\n\n".join(entries)


def reports(*gavs_and_module):
    reported = {}
    for gav, module in gavs_and_module:
        reported.setdefault(check_licenses.get_dep_key(*gav.split(":")), set()).add(module)
    return reported


class RefreshVersionsTest(unittest.TestCase):
    def refresh(self, text, reported):
        return refresh_license_versions.refresh_versions(text, reported)

    def assertFlagged(self, count, text, reported):
        new_text, updated, flagged = self.refresh(text, reported)
        self.assertEqual((text, [], count), (new_text, updated, len(flagged)))

    def test_updates_only_the_version_line_when_all_libraries_moved(self):
        notice = "notice: |\n  version: not this one\n"
        text = licenses_yaml(entry("Foo", "java-core", "1.0.0", ["org.foo: a", "org.foo: b"], notice))
        new_text, updated, flagged = self.refresh(text, reports(("org.foo:a:1.0.1", "java-core"), ("org.foo:b:1.0.1", "java-core")))
        self.assertEqual(text.replace("version: 1.0.0\n", "version: 1.0.1\n"), new_text)
        self.assertEqual([("Foo", "java-core", "1.0.0", "1.0.1")], updated)
        self.assertEqual([], flagged)

    def test_does_not_split_documents_at_indented_dashes(self):
        text = licenses_yaml("name: Foo\nnotice: |\n  ---\nmodule: java-core\nversion: 1.0.0\nlibraries:\n  - org.foo: a\n")
        _, updated, _ = self.refresh(text, reports(("org.foo:a:1.0.1", "java-core")))
        self.assertEqual([("Foo", "java-core", "1.0.0", "1.0.1")], updated)

    def test_keeps_quotes_and_quotes_versions_yaml_would_read_as_numbers(self):
        text = licenses_yaml(
            entry("Quoted", "java-core", "'2.21'", ["org.q: a"]),
            entry("Plain", "java-core", "1.0.0", ["org.p: a"]),
        )
        new_text, _, _ = self.refresh(text, reports(("org.q:a:2.22", "java-core"), ("org.p:a:2", "java-core")))
        self.assertIn("version: '2.22'\n", new_text)
        self.assertIn('version: "2"\n', new_text)

    def test_leaves_current_entries_alone(self):
        text = licenses_yaml(entry("Foo", "java-core", "1.0", ["org.foo: a"]))
        self.assertEqual((text, [], []), self.refresh(text, reports(("org.foo:a:1.0", "java-core"))))

    def test_flags_libraries_that_moved_to_different_versions(self):
        text = licenses_yaml(entry("Foo", "java-core", "1.0", ["org.foo: a", "org.foo: b"]))
        self.assertFlagged(1, text, reports(("org.foo:a:1.1", "java-core"), ("org.foo:b:1.2", "java-core")))

    def test_flags_entries_where_only_some_libraries_moved(self):
        text = licenses_yaml(entry("Foo", "java-core", "1.0", ["org.foo: a", "org.foo: b"]))
        self.assertFlagged(1, text, reports(("org.foo:a:1.1", "java-core"), ("org.foo:b:1.0", "java-core")))

    def test_updates_the_same_library_in_every_module(self):
        text = licenses_yaml(
            entry("Pg", "extensions/druid-lookups-cached-single", "42.7.13", ["org.postgresql: postgresql"]),
            entry("Pg", "extensions/druid-lookups-cached-global", "42.7.13", ["org.postgresql: postgresql"]),
        )
        _, updated, flagged = self.refresh(text, reports(
            ("org.postgresql:postgresql:42.7.14", "extensions/druid-lookups-cached-single"),
            ("org.postgresql:postgresql:42.7.14", "extensions/druid-lookups-cached-global"),
        ))
        self.assertEqual(2, len(updated))
        self.assertEqual([], flagged)

    def test_does_not_take_a_version_another_entry_registers(self):
        text = licenses_yaml(
            entry("Old", "java-core", "1.2.2", ["jakarta.activation: jakarta.activation-api"]),
            entry("New", "java-core", "2.1.4", ["jakarta.activation: jakarta.activation-api"]),
        )
        _, updated, flagged = self.refresh(text, reports(
            ("org.other:x:1.0", "java-core"),
            ("jakarta.activation:jakarta.activation-api:1.2.2", "extensions/druid-hdfs-storage"),
            ("jakarta.activation:jakarta.activation-api:2.1.5", AZURE),
        ))
        self.assertEqual([("New", "java-core", "2.1.4", "2.1.5")], updated)
        self.assertEqual([], flagged)

    def test_updates_an_entry_whose_old_version_only_ships_in_another_module(self):
        text = licenses_yaml(entry("JTS", "extensions/druid-parquet-extensions", "1.19.0", ["org.locationtech.jts: jts-core"]))
        _, updated, _ = self.refresh(text, reports(
            ("org.locationtech.jts:jts-core:1.19.0", "java-core"),
            ("org.locationtech.jts:jts-core:1.20.0", "extensions/druid-parquet-extensions"),
        ))
        self.assertEqual([("JTS", "extensions/druid-parquet-extensions", "1.19.0", "1.20.0")], updated)

    def test_leaves_entries_whose_library_is_only_reported_at_another_entry_version(self):
        text = licenses_yaml(
            entry("Stale", "java-core", "1.0", ["org.foo: a"]),
            entry("Current", "java-core", "2.0", ["org.foo: a"]),
        )
        self.assertEqual((text, [], []), self.refresh(text, reports(("org.foo:a:2.0", "java-core"))))

    def test_takes_a_version_an_entry_of_another_module_registers(self):
        text = licenses_yaml(
            entry("Auth", "java-core", "1.30.0", ["com.google.auth: google-auth-library-credentials"]),
            entry("Auth", "extensions/druid-google-extensions", "1.54.0", ["com.google.auth: google-auth-library-credentials"]),
        )
        _, updated, flagged = self.refresh(text, reports(
            ("com.google.auth:google-auth-library-credentials:1.54.0", "java-core"),
            ("com.google.auth:google-auth-library-credentials:1.54.0", "extensions/druid-google-extensions"),
        ))
        self.assertEqual([("Auth", "java-core", "1.30.0", "1.54.0")], updated)
        self.assertEqual([], flagged)

    def test_flags_entries_of_one_module_that_move_to_the_same_version(self):
        text = licenses_yaml(
            entry("Kafka", "extensions/druid-avro-extensions", "6.2.12-ccs", ["org.apache.kafka: kafka-clients"]),
            entry("Kafka", "extensions/druid-avro-extensions", "5.5.1-ccs", ["org.apache.kafka: kafka-clients"]),
        )
        self.assertFlagged(2, text, reports(("org.apache.kafka:kafka-clients:8.3.2-ccs", "extensions/druid-avro-extensions")))

    def test_flags_moved_entries_with_libraries_in_no_report(self):
        text = licenses_yaml(entry("Kotlin jdk8", "java-core", "1.8.21", ["org.k: stdlib", "org.k: stdlib-jdk8"]))
        self.assertFlagged(1, text, reports(("org.k:stdlib:2.4.21", "java-core")))

    def test_flags_moved_entries_with_libraries_another_entry_registers(self):
        text = licenses_yaml(
            entry("Family", "java-core", "1.0.0", ["org.f: a", "org.f: b"]),
            entry("B", "java-core", "1.0.1", ["org.f: b"]),
        )
        self.assertFlagged(1, text, reports(("org.f:a:1.0.1", "java-core"), ("org.f:b:1.0.1", "java-core")))

    def test_flags_a_library_with_several_new_versions(self):
        text = licenses_yaml(entry("Foo", "java-core", "1.0", ["org.foo: a"]))
        self.assertFlagged(1, text, reports(("org.other:x:1.0", "java-core"), ("org.foo:a:1.1", AZURE), ("org.foo:a:1.2", "extensions/druid-hdfs-storage")))

    def test_prefers_the_version_reported_in_the_entry_module(self):
        text = licenses_yaml(entry("Kinesis", "extensions/druid-kinesis-indexing-service", "1.12.3", ["com.amazonaws: jmespath-java"]))
        _, updated, _ = self.refresh(text, reports(
            ("com.amazonaws:jmespath-java:1.12.800", "java-core"),
            ("com.amazonaws:jmespath-java:1.12.4", "extensions/druid-kinesis-indexing-service"),
        ))
        self.assertEqual([("Kinesis", "extensions/druid-kinesis-indexing-service", "1.12.3", "1.12.4")], updated)

    def test_skips_flagged_entries_and_modules_without_a_report(self):
        text = licenses_yaml(
            entry("Skipped", "java-core", "1.0", ["org.s: a"], "skip_dependency_report_check: true\n"),
            entry("Contrib", "extensions-contrib/druid-ranger-security", "1.0", ["org.c: a"]),
        )
        self.assertEqual((text, [], []), self.refresh(text, reports(("org.s:a:1.1", "java-core"), ("org.c:a:1.1", "java-core"))))


if __name__ == "__main__":
    unittest.main()
