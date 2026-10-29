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

import contextlib
import importlib.util
import io
import os
import tempfile
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


class SplitStaleVersionsTest(unittest.TestCase):
    MIT = "MIT License"
    NEW = check_licenses.get_dep_key("org.example", "a", "1.1")

    def split(self, registered, reporting_modules=(AZURE,), reported_license=APACHE, also_reported=()):
        # registered: (version, license, modules) of each org.example:a registration;
        # also_reported: "g:a:v" and module pairs reported next to org.example:a:1.1.
        registered_dep_to_licenses = {}
        registered_modules = {}
        for version, license, modules in registered:
            key = check_licenses.get_dep_key("org.example", "a", version)
            registered_dep_to_licenses[key] = license
            registered_modules[key] = set(modules)
        reported_modules = reports(*also_reported)
        reported_modules[self.NEW] = set(reporting_modules)
        missing = [(", ".join(reporting_modules),) + self.NEW + (reported_license,)]
        return check_licenses.split_stale_versions(
            missing, reported_modules, registered_dep_to_licenses, registered_modules, EXCLUDED
        )

    def assertStale(self, registered_versions, split, module=AZURE):
        self.assertEqual(([(module,) + self.NEW + (APACHE, registered_versions)], []), split)

    def assertMissing(self, split, reported_license=APACHE):
        self.assertEqual(([], [(AZURE,) + self.NEW + (reported_license,)]), split)

    def test_registered_under_another_version_with_the_same_license_is_stale(self):
        self.assertStale("1.0", self.split([("1.0", APACHE, [AZURE])]))

    def test_registered_under_another_version_of_another_module_is_stale(self):
        self.assertStale("1.0", self.split([("1.0", APACHE, ["java-core"])]))

    def test_unregistered_is_missing(self):
        self.assertMissing(self.split([]))

    def test_registered_under_another_version_with_another_license_is_missing(self):
        self.assertMissing(self.split([("1.0", self.MIT, [AZURE])]))

    def test_registered_under_versions_with_different_licenses_is_missing(self):
        self.assertMissing(self.split([("1.0", APACHE, [AZURE]), ("0.9", self.MIT, ["java-core"])]))

    def test_registered_under_several_versions_with_the_same_license_is_stale(self):
        self.assertStale("0.9, 1.0", self.split([("1.0", APACHE, [AZURE]), ("0.9", APACHE, ["java-core"])]))

    def test_registered_only_under_excluded_module_is_missing_when_shipped(self):
        self.assertMissing(self.split([("1.0", APACHE, [RANGER])]))

    def test_registrations_under_excluded_modules_are_ignored_when_shipped(self):
        self.assertStale("1.0", self.split([("1.0", APACHE, [AZURE]), ("0.9", self.MIT, [RANGER])]))

    def test_registered_under_excluded_module_is_stale_when_not_shipped(self):
        self.assertStale(
            "1.0", self.split([("1.0", APACHE, [TESTCONTAINERS])], reporting_modules=(TESTCONTAINERS,)), module=TESTCONTAINERS
        )

    def test_registered_version_still_reported_is_missing(self):
        # A module that starts bundling another version of a library is not a version bump.
        self.assertMissing(self.split([("1.0", APACHE, ["java-core"])], also_reported=[("org.example:a:1.0", "java-core")]))

    def test_one_registered_version_no_longer_reported_is_stale(self):
        self.assertStale("0.9, 1.0", self.split(
            [("1.0", APACHE, [AZURE]), ("0.9", APACHE, ["java-core"])], also_reported=[("org.example:a:0.9", "java-core")]
        ))

    def test_unknown_reported_license_is_missing(self):
        self.assertMissing(self.split([("1.0", APACHE, [AZURE])], reported_license=None), reported_license=None)


def find_unknown(license, reporting_modules, skipping=None):
    return check_licenses.find_unknown_licenses(
        {GAV: (license, reporting_modules[0])}, {GAV: set(reporting_modules)}, {GAV: ["Some New License"]}, skipping or {}, EXCLUDED
    )


class FindUnknownLicensesTest(unittest.TestCase):
    def test_reported_by_released_module(self):
        self.assertEqual([(AZURE,) + GAV + ("Some New License",)], find_unknown(None, [AZURE]))

    def test_reported_only_by_excluded_modules_is_ignored(self):
        self.assertEqual([], find_unknown(None, [TESTCONTAINERS, RANGER]))

    def test_reported_by_released_and_excluded_modules(self):
        self.assertEqual([(AZURE,) + GAV + ("Some New License",)], find_unknown(None, [TESTCONTAINERS, AZURE]))

    def test_known_and_unspecified_licenses_are_ignored(self):
        self.assertEqual([], find_unknown(APACHE, [AZURE]))
        self.assertEqual([], find_unknown("-", [AZURE]))

    def test_skipped_dependency_is_ignored(self):
        self.assertEqual([], find_unknown(None, [AZURE], skipping={GAV: {}}))


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

    def test_keeps_quotes_and_does_not_quote_versions_that_look_like_numbers(self):
        text = licenses_yaml(
            entry("Quoted", "java-core", "'2.21'", ["org.q: a"]),
            entry("Plain", "java-core", "1.0.0", ["org.p: a"]),
        )
        new_text, _, _ = self.refresh(text, reports(("org.q:a:2.22", "java-core"), ("org.p:a:2.10", "java-core")))
        self.assertIn("version: '2.22'\n", new_text)
        self.assertIn("version: 2.10\n", new_text)

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


def report_html(*licenses):
    row = ", ".join("<a>{}</a>".format(license) for license in licenses)
    return (
        '<a id="Project_Dependencies_compile"></a><table>'
        "<tr><th>GroupId</th><th>ArtifactId</th><th>Version</th><th>Type</th><th>Licenses</th></tr>"
        "<tr><td>org.tukaani</td><td>xz</td><td>1.10</td><td>jar</td><td>{}</td></tr>"
        "</table>".format(row)
    )


def parse_report(*licenses):
    parser = check_licenses.DependencyReportParser("java-core", check_licenses.build_compatible_license_names())
    dep_to_license = parser.parse(io.StringIO(report_html(*licenses)))
    return dep_to_license, parser.dep_to_license_names


class DependencyReportParserTest(unittest.TestCase):
    XZ = check_licenses.get_dep_key("org.tukaani", "xz", "1.10")

    def license_of(self, *licenses):
        return parse_report(*licenses)[0][self.XZ][0]

    def test_known_license_is_normalized(self):
        self.assertEqual(({self.XZ: ("Zero-Clause BSD", "java-core")}, {self.XZ: ["0BSD"]}), parse_report("0BSD"))

    def test_unknown_license_is_unset_and_its_name_is_kept(self):
        self.assertEqual(({self.XZ: (None, "java-core")}, {self.XZ: ["Some New License"]}), parse_report("Some New License"))

    def test_unknown_license_is_fine_when_another_license_is_known(self):
        self.assertEqual(APACHE, self.license_of("Some New License", "Apache-2.0"))

    def test_gpl_only_license_is_unset(self):
        self.assertIsNone(self.license_of("GNU Lesser General Public License, Version 2.1"))

    def test_gpl_license_does_not_override_a_compatible_one(self):
        self.assertEqual("Eclipse Public License 1.0", self.license_of("LGPL-2.1", "Eclipse Public License - v 1.0", "LGPL-2.1"))

    def test_known_license_mentioning_gpl_is_normalized(self):
        self.assertEqual("CDDL 1.1", self.license_of("CDDL + GPLv2 with classpath exception"))

    def test_separators_are_not_license_names(self):
        self.assertEqual({self.XZ: ["Some New License", "LGPL-2.1"]}, parse_report("Some New License", "LGPL-2.1")[1])

    def test_unspecified_license_is_kept(self):
        self.assertEqual("-", self.license_of("-", "Apache-2.0"))

    def test_first_compatible_license_is_kept(self):
        self.assertEqual("MIT No Attribution", self.license_of("MIT-0", "Apache-2.0"))


class CompatibleLicenseNamesTest(unittest.TestCase):
    def test_every_normalized_name_is_a_name(self):
        compatible_license_names = check_licenses.build_compatible_license_names()
        self.assertEqual([], sorted(set(compatible_license_names.values()) - set(compatible_license_names)))


class CheckLicensesTest(unittest.TestCase):
    # The report lists org.tukaani:xz:1.10 under the Apache license.

    def check(self, registered_entry, stale_versions):
        # Returns the stderr output and the exit code, None when the check passes.
        with tempfile.TemporaryDirectory() as root:
            os.makedirs(os.path.join(root, "licenses", "bin"))
            os.makedirs(os.path.join(root, "reports", "core"))
            with open(os.path.join(root, "licenses.yaml"), "w", encoding="utf-8") as f:
                f.write(licenses_yaml(registered_entry))
            with open(os.path.join(root, "reports", "core", "dependencies.html"), "w", encoding="utf-8") as f:
                f.write(report_html("Apache-2.0"))
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                try:
                    check_licenses.check_licenses(
                        os.path.join(root, "licenses.yaml"), os.path.join(root, "reports"), EXCLUDED, stale_versions
                    )
                except SystemExit as e:
                    return stderr.getvalue(), e.code
            return stderr.getvalue(), None

    def assertMissing(self, registered_entry, stale_versions):
        output, code = self.check(registered_entry, stale_versions)
        self.assertEqual(1, code)
        self.assertIn("Error: found 1 missing licenses", output)

    def test_stale_version_fails_in_fail_mode(self):
        output, code = self.check(entry("XZ", "java-core", "1.9", ["org.tukaani: xz"]), "fail")
        self.assertEqual(1, code)
        self.assertIn("Error: found 1 dependencies whose previous versions are registered", output)
        self.assertIn("refresh-license-versions.py licenses.yaml target/license-reports", output)

    def test_stale_version_only_warns_in_warn_mode(self):
        output, code = self.check(entry("XZ", "java-core", "1.9", ["org.tukaani: xz"]), "warn")
        self.assertIsNone(code)
        self.assertIn("version: 1.10, license: Apache License version 2.0, registered_versions: 1.9", output)
        self.assertIn("refresh-license-versions.py licenses.yaml target/license-reports", output)

    def test_unregistered_dependency_fails_in_warn_mode(self):
        self.assertMissing(entry("Other", "java-core", "1.9", ["org.example: other"]), "warn")


class LoadLicensesYamlTest(unittest.TestCase):
    def test_versions_that_look_like_numbers_are_loaded_as_strings(self):
        docs = check_licenses.licenses_yaml.load_all("version: 1.10\n---\nversion: 2\n---\nversion: 1.5.7-11\n")
        self.assertEqual(["1.10", "2", "1.5.7-11"], [doc["version"] for doc in docs])

    def test_booleans_are_still_loaded_as_booleans(self):
        self.assertEqual([{"skip_dependency_report_check": True}], check_licenses.licenses_yaml.load_all("skip_dependency_report_check: true\n"))


if __name__ == "__main__":
    unittest.main()
