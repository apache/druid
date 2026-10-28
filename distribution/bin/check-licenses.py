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

import licenses_yaml
import os
import sys
from html.parser import HTMLParser
import argparse

class DependencyReportParser(HTMLParser):
    # This class parses the given html file to find all dependency reports under "Project dependencies"
    # and "Projection transparent dependencies" sections.
    # The parser works based on the state machine and its state is updated whenever it reads a new tag.
    # The state changes as below:
    #
    # none -> h2_start -> project_dependencies_start -> h3_start -> compile_start -> table_start -> row_start -> th_start / td_start -> th_end / td_end -> row_end -> table_end -> compile_end -> h3_end -> project_dependencies_end -> h2_end -> none

    attr_index = 0
    group_id = None
    artifact_id = None
    version = None
    classifier = None
    dep_type = None
    license = None
    state = "none"
    dep_to_license = None
    compatible_license_names = None
    include_classifier = False
    druid_module_name = None

    def __init__(self, druid_module_name, compatible_license_names):
        HTMLParser.__init__(self)
        self.state = "none"
        self.druid_module_name = druid_module_name
        self.compatible_license_names = compatible_license_names

    def parse(self, f):
        self.dep_to_license = {}
        self.unsupported_licenses = []
        self.tables_started = 0
        self.tables_finished = 0
        self.feed(f.read())
        if self.tables_finished < self.tables_started:
            raise Exception("Parsed only {} of {} dependency tables".format(self.tables_finished, self.tables_started))
        return self.dep_to_license

    def handle_starttag(self, tag, attrs):
        # print("current: {}, start tag: {}, attrs:{} ".format(self.state, tag, attrs))
        if tag == "a" and dict(attrs).get("id") in ("Project_Dependencies_compile", "Project_Transitive_Dependencies_compile",
                                                    "Project_Dependencies_runtime", "Project_Transitive_Dependencies_runtime"):
            self.state = "h3_end"
            self.include_classifier = False
            self.tables_started += 1

        if self.state == "none":
            if tag == "h2":
                self.state = "h2_start"

        if self.state == "h2_start":
            if tag == "a":
                for attr in attrs:
                    if attr[0] == "name" and (attr[1] == "Project_Dependencies" or attr[1] == "Project_Transitive_Dependencies"):
                        self.state = "project_dependencies_start"
                        self.include_classifier = False

        if self.state == "h2_end":
            if tag == "h3":
                self.state = "h3_start"

        if self.state == "h3_start":
            if tag == "a":
                for attr in attrs:
                    if attr[0] == "name" and attr[1] == "compile":
                        self.state = "compile_start"

        if self.state == "h3_end":
            if tag == "table":
                self.state = "table_start"

        if self.state == "table_start":
            if tag == "tr":
                self.state = "row_start"
                self.clear_attr()

        if self.state == "row_end":
            if tag == "tr":
                self.state = "row_start"
                self.clear_attr()

        if self.state == "row_start":
            if tag == "td":
                self.state = "td_start"
            elif tag == "th":
                self.state = "th_start"

        if self.state == "th_end":
            if tag == "th":
                self.state = "th_start"

        if self.state == "td_end":
            if tag == "td":
                self.state = "td_start"

    def handle_endtag(self, tag):
        # print("current: {}, end tag: {}".format(self.state, tag))
        if self.state == "project_dependencies_start":
            if tag == "a":
                self.state = "project_dependencies_end"

        if self.state == "h2_start":
            if tag == "h2":
                self.state = "h2_end"

        if self.state == "project_dependencies_end":
            if tag == "h2":
                self.state = "h2_end"

        if self.state == "compile_start":
            if tag == "a":
                self.state = "compile_end"

        if self.state == "compile_end":
            if tag == "h3":
                self.state = "h3_end"

        if self.state == "table_start":
            if tag == "table":
                self.state = "none"

        if self.state == "td_start":
            if tag == "td":
                self.state = "td_end"
                self.attr_index = self.attr_index + 1

        if self.state == "th_start":
            if tag == "th":
                self.state = "th_end"

        if self.state == "row_start":
            if tag == "tr":
                self.state = "row_end"

        if self.state == "th_end":
            if tag == "tr":
                self.state = "row_end"

        if self.state == "td_end":
            if tag == "tr":
                self.state = "row_end"
                # print(json.dumps({"groupId": self.group_id, "artifactId": self.artifact_id, "version": self.version, "classifier": self.classifier, "type": self.dep_type, "license": self.license}))
                if self.group_id.find("org.apache.druid") < 0:
                    self.dep_to_license[get_dep_key(self.group_id, self.artifact_id, self.version)] = (self.license, self.druid_module_name)
                    # A license name missing from compatible_license_names would otherwise leave the license unset,
                    # which silently skips the comparison against licenses.yaml.
                    if self.license is None and self.unsupported_license_names:
                        self.unsupported_licenses.append((self.druid_module_name, self.group_id, self.artifact_id, self.version, ", ".join(self.unsupported_license_names)))

        if self.state == "row_end":
            if tag == "table":
                self.state = "none"
                self.tables_finished += 1

    def handle_data(self, data):
        if self.state == "td_start":
            self.set_attr(data)
        elif self.state == "th_start":
            if data.lower() == "classifier":
                self.include_classifier = True

    def clear_attr(self):
        self.group_id = None
        self.artifact_id = None
        self.version = None
        self.classifier = None
        self.dep_type = None
        self.license = None
        self.unsupported_license_names = []
        self.attr_index = 0

    def set_attr(self, data):
        #print("set data: {}".format(data))
        if self.attr_index == 0:
            self.group_id = data
        elif self.attr_index == 1:
            self.artifact_id = data
        elif self.attr_index == 2:
            self.version = get_version_string(data)
        elif self.attr_index == 3:
            if self.include_classifier:
                self.classifier = data
            else:
                self.dep_type = data
        elif self.attr_index == 4:
            if self.include_classifier:
                self.dep_type = data
            else:
                self.set_license(data)
        elif self.attr_index == 5:
            if self.include_classifier:
                self.set_license(data)
            else:
                raise Exception("Unknown attr_index [{}]".format(self.attr_index))
        else:
            raise Exception("Unknown attr_index [{}]".format(self.attr_index))

    def set_license(self, data):
        if data.upper().find("GPL") < 0:
            # Check if the license assosciated with the component is acccepted
            # set_license() will pick the first acceptable license
            # this fixes issue where a multi-licensed component
            # could override accepted license with not accepted one
            # e.g., EPL / GPL for logback-core
            if self.license not in self.compatible_license_names:
                try:
                    self.license = self.compatible_license_names[data]
                except KeyError:
                    self.unsupported_license_names.append(data)
            else:
                print(self.group_id + " "  + self.artifact_id + " in: " + self.druid_module_name + " with: " + self.license + " ignoring " + data)


def print_log_to_stderr(string):
    print(string, file=sys.stderr)

def build_compatible_license_names():
    compatible_licenses = {}
    compatible_licenses['Apache License, Version 2.0'] = 'Apache License version 2.0'
    compatible_licenses['The Apache Software License, Version 2.0'] = 'Apache License version 2.0'
    compatible_licenses['Apache 2.0'] = 'Apache License version 2.0'
    compatible_licenses['Apache-2.0'] = 'Apache License version 2.0'
    compatible_licenses['Apache 2'] = 'Apache License version 2.0'
    compatible_licenses['Apache License 2'] = 'Apache License version 2.0'
    compatible_licenses['Apache License 2.0'] = 'Apache License version 2.0'
    compatible_licenses['Apache Software License - Version 2.0'] = 'Apache License version 2.0'
    compatible_licenses['The Apache License, Version 2.0'] = 'Apache License version 2.0'
    compatible_licenses['Apache License version 2.0'] = 'Apache License version 2.0'
    compatible_licenses['Apache License Version 2.0'] = 'Apache License version 2.0'
    compatible_licenses['Apache License Version 2'] = 'Apache License version 2.0'
    compatible_licenses['Apache License v2.0'] = 'Apache License version 2.0'
    compatible_licenses['Apache License, 2.0'] = 'Apache License version 2.0'
    compatible_licenses['Apache License, version 2.0'] = 'Apache License version 2.0'
    compatible_licenses['Apache 2.0 License'] = 'Apache License version 2.0'
    compatible_licenses['Apache License, 2.0'] = 'Apache License version 2.0'

    compatible_licenses['Public Domain'] = 'Public Domain'

    compatible_licenses['BSD-2-Clause License'] = 'BSD-2-Clause License'
    compatible_licenses['BSD-2-Clause'] = 'BSD-2-Clause License'
    compatible_licenses['BSD 2-Clause license'] = 'BSD-2-Clause License'
    compatible_licenses['BSD 2-Clause License'] = 'BSD-2-Clause License'
    compatible_licenses['The BSD 2-Clause License'] = 'BSD-2-Clause License'

    compatible_licenses['BSD-3-Clause License'] = 'BSD-3-Clause License'
    compatible_licenses['New BSD license'] = 'BSD-3-Clause License'
    compatible_licenses['BSD'] = 'BSD-3-Clause License'
    compatible_licenses['The BSD License'] = 'BSD-3-Clause License'
    compatible_licenses['BSD licence'] = 'BSD-3-Clause License'
    compatible_licenses['BSD License'] = 'BSD-3-Clause License'
    compatible_licenses['BSD-like'] = 'BSD-3-Clause License'
    compatible_licenses['BSD 3-clause'] = 'BSD-3-Clause License'
    compatible_licenses['The BSD 3-Clause License'] = 'BSD-3-Clause License'
    compatible_licenses['Revised BSD'] = 'BSD-3-Clause License'
    compatible_licenses['New BSD License'] = 'BSD-3-Clause License'
    compatible_licenses['BSD New license'] = 'BSD-3-Clause License'
    compatible_licenses['3-Clause BSD License'] = 'BSD-3-Clause License'
    compatible_licenses['BSD 3-Clause'] = 'BSD-3-Clause License'
    compatible_licenses['BSD-3-Clause'] = 'BSD-3-Clause License'

    compatible_licenses['Unicode/ICU License'] = 'Unicode/ICU License'
    compatible_licenses['Unicode-3.0'] = 'Unicode/ICU License'

    compatible_licenses['SIL Open Font License 1.1'] = 'SIL Open Font License 1.1'

    compatible_licenses['CDDL 1.1'] = 'CDDL 1.1'
    compatible_licenses['CDDL/GPLv2+CE'] = 'CDDL 1.1'
    compatible_licenses['CDDL + GPLv2 with classpath exception'] = 'CDDL 1.1'
    compatible_licenses['CDDL License'] = 'CDDL 1.1'
    compatible_licenses['COMMON DEVELOPMENT AND DISTRIBUTION LICENSE (CDDL) Version 1.0'] = 'CDDL 1.0'

    compatible_licenses['Eclipse Public License 1.0'] = 'Eclipse Public License 1.0'
    compatible_licenses['The Eclipse Public License, Version 1.0'] = 'Eclipse Public License 1.0'
    compatible_licenses['Eclipse Public License - Version 1.0'] = 'Eclipse Public License 1.0'
    compatible_licenses['Eclipse Public License, Version 1.0'] = 'Eclipse Public License 1.0'
    compatible_licenses['Eclipse Public License v1.0'] = 'Eclipse Public License 1.0'
    compatible_licenses['Eclipse Public License - v1.0'] = 'Eclipse Public License 1.0'
    compatible_licenses['Eclipse Public License - v 1.0'] = 'Eclipse Public License 1.0'
    compatible_licenses['EPL 1.0'] = 'Eclipse Public License 1.0'

    compatible_licenses['Eclipse Public License 2.0'] = 'Eclipse Public License 2.0'
    compatible_licenses['The Eclipse Public License, Version 2.0'] = 'Eclipse Public License 2.0'
    compatible_licenses['Eclipse Public License - Version 2.0'] = 'Eclipse Public License 2.0'
    compatible_licenses['Eclipse Public License, Version 2.0'] = 'Eclipse Public License 2.0'
    compatible_licenses['Eclipse Public License v2.0'] = 'Eclipse Public License 2.0'
    compatible_licenses['Eclipse Public License v. 2.0'] = 'Eclipse Public License 2.0'
    compatible_licenses['EPL 2.0'] = 'Eclipse Public License 2.0'
    compatible_licenses['EPL-2.0'] = 'Eclipse Public License 2.0'

    compatible_licenses['Eclipse Distribution License 1.0'] = 'Eclipse Distribution License 1.0'
    compatible_licenses['Eclipse Distribution License - v 1.0'] = 'Eclipse Distribution License 1.0'
    compatible_licenses['Eclipse Distribution License v. 1.0'] = 'Eclipse Distribution License 1.0'
    compatible_licenses['EDL 1.0'] = 'Eclipse Distribution License 1.0'

    compatible_licenses['Mozilla Public License Version 2.0'] = 'Mozilla Public License Version 2.0'
    compatible_licenses['Mozilla Public License, Version 2.0'] = 'Mozilla Public License Version 2.0'

    compatible_licenses['MPL 1.1'] = 'MPL 1.1'
    compatible_licenses['Mozilla Public License 1.1'] = 'MPL 1.1'

    compatible_licenses['Creative Commons Attribution 2.5'] = 'Creative Commons Attribution 2.5'

    compatible_licenses['Creative Commons CC0'] = 'Creative Commons CC0'
    compatible_licenses['CC0'] = 'Creative Commons CC0'
    compatible_licenses['Public Domain, per Creative Commons CC0'] = 'Creative Commons CC0'

    compatible_licenses['The MIT License'] = 'MIT License'
    compatible_licenses['MIT License'] = 'MIT License'
    compatible_licenses['The MIT License (MIT)'] = 'MIT License'
    compatible_licenses['Bouncy Castle Licence'] = 'MIT License'
    compatible_licenses['SPDX-License-Identifier: MIT'] = 'MIT License'
    compatible_licenses['MIT'] = 'MIT License'
    compatible_licenses['MIT license'] = 'MIT License'

    compatible_licenses['MIT-0'] = 'MIT No Attribution'

    compatible_licenses['Zero-Clause BSD'] = 'Zero-Clause BSD'
    compatible_licenses['0BSD'] = 'Zero-Clause BSD'

    compatible_licenses['The Go license'] = 'The Go license'
    compatible_licenses['Go License'] = 'The Go license'
    compatible_licenses['Universal Permissive License, Version 1.0'] = 'Universal Permissive License, Version 1.0'

    compatible_licenses['-'] = '-'
    return compatible_licenses

def get_dep_key(group_id, artifact_id, version):
    return (group_id, artifact_id, version)

def get_version_string(version):
    if type(version) == str:
        return version
    else:
        return str(version)

def find_druid_module_name(dirpath):
    # Returns the licenses.yaml module tag of the report: extensions/<artifactId> or java-core.
    ext_start = dirpath.find("/ext/")
    if ext_start > 0:
        # Found an extension
        subpath = dirpath[(len("/ext/") + ext_start):]
        ext_name_end = subpath.find("/")
        if ext_name_end < 0:
            raise Exception("Can't determine extension name from [{}]".format(dirpath))
        else:
            return "extensions/" + subpath[0:ext_name_end]
    else:
        # Druid core
        return "java-core"

def check_license_files(license_yaml, licenses_list):
    # Every license_file_path must exist, and every file in licenses/bin must be referenced by a license_file_path,
    # because the binary distribution ships the whole licenses/ directory.
    repo_root = os.path.dirname(os.path.abspath(license_yaml))
    referenced_files = set()
    for license in licenses_list:
        license_file_path = license.get('license_file_path')
        if license_file_path:
            referenced_files.update(license_file_path if type(license_file_path) == list else [license_file_path])

    missing_files = sorted(f for f in referenced_files if not os.path.isfile(os.path.join(repo_root, f)))
    bin_files = {'licenses/bin/' + f for f in os.listdir(os.path.join(repo_root, 'licenses', 'bin')) if not f.startswith('.')}
    unreferenced_files = sorted(bin_files - referenced_files)

    if missing_files:
        print_log_to_stderr("Error: found {} license_file_path entries pointing at missing files".format(len(missing_files)))
        for f in missing_files:
            print_log_to_stderr("  {}".format(f))
    if unreferenced_files:
        print_log_to_stderr("Error: found {} files in licenses/bin that no license_file_path references. Delete them or reference them in licenses.yaml.".format(len(unreferenced_files)))
        for f in unreferenced_files:
            print_log_to_stderr("  {}".format(f))
    if missing_files or unreferenced_files:
        sys.exit(1)

def is_excluded_module(module, exclude_module_prefixes):
    return any(module.startswith(prefix) for prefix in exclude_module_prefixes)

def find_missing_licenses(reported_dep_to_licenses, reported_modules, registered_modules, skipping_licenses, exclude_module_prefixes):
    # A dependency that ships in the release needs at least one entry outside the excluded modules.
    missing_licenses = []
    for key, reported_license_druid_module in reported_dep_to_licenses.items():
        if reported_license_druid_module[0] == "-" or key in skipping_licenses:
            continue
        shipped_modules = sorted(m for m in reported_modules[key] if not is_excluded_module(m, exclude_module_prefixes))
        released = any(not is_excluded_module(m, exclude_module_prefixes) for m in registered_modules.get(key, ()))
        if key not in registered_modules:
            modules = sorted(reported_modules[key])
        elif shipped_modules and not released:
            modules = shipped_modules
        else:
            continue
        missing_licenses.append((", ".join(modules),) + key + (reported_license_druid_module[0],))
    return missing_licenses

def find_unsupported_licenses(unsupported_licenses, exclude_module_prefixes):
    # An unknown license only fails the check when it is reported by a module that ships in the release.
    return [u for u in unsupported_licenses if not is_excluded_module(u[0], exclude_module_prefixes)]

def read_dependency_reports(dependency_reports_root, compatible_license_names):
    # Returns (group_id, artifact_id, version) -> (license, druid_module), (group_id, artifact_id, version) -> modules,
    # and the dependencies whose reported licenses are all unknown, as (druid_module, group_id, artifact_id, version, licenses).
    reported_dep_to_licenses = {}
    reported_modules = {}
    unsupported_licenses = []
    for dirpath, dirnames, filenames in os.walk(dependency_reports_root):
        for filename in filenames:
            if filename == "dependencies.html":
                full_path = os.path.join(dirpath, filename)
                # Determine if it's druid core or an extension
                druid_module_name = find_druid_module_name(dirpath)
                print_log_to_stderr("Parsing {}".format(full_path))
                with open(full_path, encoding="utf-8") as report_file:
                    parser = DependencyReportParser(druid_module_name, compatible_license_names)
                    dep_to_license = parser.parse(report_file)
                    reported_dep_to_licenses.update(dep_to_license)
                    unsupported_licenses.extend(parser.unsupported_licenses)
                    for key in dep_to_license:
                        reported_modules.setdefault(key, set()).add(druid_module_name)
    if len(reported_dep_to_licenses) == 0:
        raise Exception("No dependency reports are found")
    return reported_dep_to_licenses, reported_modules, unsupported_licenses

def check_licenses(license_yaml, dependency_reports_root, exclude_module_prefixes):
    # Build a dictionary to facilitate comparing reported licenses and registered ones.
    # These dictionaries are the mapping of (group_id, artifact_id, version) to license_name.

    with open(license_yaml, encoding='utf-8') as registry_file:
        licenses_list = licenses_yaml.load_all(registry_file)
    check_license_files(license_yaml, licenses_list)

    # Build reported license dictionary.
    compatible_license_names = build_compatible_license_names()
    reported_dep_to_licenses, reported_modules, unsupported_licenses = read_dependency_reports(
        dependency_reports_root, compatible_license_names
    )

    unsupported_licenses = find_unsupported_licenses(unsupported_licenses, exclude_module_prefixes)
    if unsupported_licenses:
        print_log_to_stderr("Error: found {} dependencies whose reported licenses are unknown".format(len(unsupported_licenses)))
        for unsupported_license in unsupported_licenses:
            print_log_to_stderr("druid_module: {}, groupId: {}, artifactId: {}, version: {}, reported_license: {}".format(*unsupported_license))
        print_log_to_stderr(
            "If the license is compatible, add its name to build_compatible_license_names() in check-licenses.py "
            "and register the dependency in licenses.yaml under that license."
        )
        sys.exit(1)

    print_log_to_stderr("Found {} reported licenses\n".format(len(reported_dep_to_licenses)))

    # Build registered license dictionary.
    registered_dep_to_licenses = {}
    registered_modules = {}
    skipping_licenses = {}
    # Validate that every entry uses a known module tag. Core extensions use extensions/<output-dir>
    # (matching how they appear in the binary distribution's extensions/ directory), and contrib
    # extensions use extensions-contrib/<output-dir> so the release build can filter them out. This
    # check enforces that any new contrib entry actually uses the contrib prefix, so it cannot
    # silently leak into the release LICENSE/NOTICE under a stray/mistagged module name.
    known_module_prefixes = (
        'extensions/',
        'extensions-contrib/',
    )
    known_module_exact = {
        'java-core',
        'web-console',
    }
    unknown_module_entries = []
    for license in licenses_list:
        module = license.get('module')
        if module is None:
            unknown_module_entries.append((license.get('name'), '<missing module>'))
        elif module not in known_module_exact and not any(module.startswith(p) for p in known_module_prefixes):
            unknown_module_entries.append((license.get('name'), module))
    if unknown_module_entries:
        print_log_to_stderr("Error: found {} licenses.yaml entries with unknown module tags".format(len(unknown_module_entries)))
        for name, module in unknown_module_entries:
            print_log_to_stderr("  name: {}, module: {}".format(name, module))
        print_log_to_stderr(
            "Every entry must use one of the known exact module tags [{}] or a module starting with one of [{}]. "
            "Contrib extensions in particular must use the extensions-contrib/ prefix so the release "
            "LICENSE/NOTICE generation can filter them out.".format(
                ', '.join(sorted(known_module_exact)),
                ', '.join(known_module_prefixes)
            )
        )
        sys.exit(1)

    unknown_license_name_entries = [
        (license.get('name'), license.get('license_name'))
        for license in licenses_list
        if 'libraries' in license and license.get('license_name') not in compatible_license_names
    ]
    if unknown_license_name_entries:
        print_log_to_stderr("Error: found {} licenses.yaml entries with unknown license names".format(len(unknown_license_name_entries)))
        for name, license_name in unknown_license_name_entries:
            print_log_to_stderr("  name: {}, license_name: {}".format(name, license_name))
        print_log_to_stderr("Every license_name must be one of the names in build_compatible_license_names() in check-licenses.py.")
        sys.exit(1)

    for license in licenses_list:
        if 'libraries' in license:
            for library in license['libraries']:
                if type(library) is not dict:
                    raise Exception("Expected dict but got {}[{}]".format(type(library), library))
                if len(library) > 1:
                    raise Exception("Expected 1 groupId and artifactId, but got [{}]".format(library))
                for group_id, artifact_id in library.items():
                    if 'version' not in license:
                        raise Exception("version is missing in {}".format(license))
                    if 'license_name' not in license:
                        raise Exception("name is missing in {}".format(license))
                    if 'skip_dependency_report_check' in license and license['skip_dependency_report_check']:
                        if 'version' not in license:
                            version = "-"
                        else:
                            version = get_version_string(license['version'])
                        skipping_licenses[get_dep_key(group_id, artifact_id, version)] = license
                    else:
                        key = get_dep_key(group_id, artifact_id, get_version_string(license['version']))
                        registered_dep_to_licenses[key] = compatible_license_names[license['license_name']]
                        registered_modules.setdefault(key, set()).add(license['module'])

    if len(registered_dep_to_licenses) == 0:
        raise Exception("No registered licenses are found")

    # Compare licenses in registry and those in dependency reports.
    mismatched_licenses = []
    unchecked_licenses = []
    # Iterate through registered licenses and check if its license is same with the reported one.
    for key, registered_license in registered_dep_to_licenses.items():
        if key in reported_dep_to_licenses: # key is (group_id, artifact_id, version)
            reported_license_druid_module = reported_dep_to_licenses[key]
            reported_license = reported_license_druid_module[0]
            druid_module = reported_license_druid_module[1]
            if reported_license is not None and reported_license != "-" and reported_license != registered_license:
                group_id = key[0]
                artifact_id = key[1]
                version = key[2]
                mismatched_licenses.append((druid_module, group_id, artifact_id, version, reported_license, registered_license))

    # If we find any mismatched license, stop immediately.
    if len(mismatched_licenses) > 0:
        print_log_to_stderr("Error: found {} mismatches between reported licenses and registered licenses".format(len(mismatched_licenses)))
        for mismatched_license in mismatched_licenses:
            print_log_to_stderr("druid_module: {}, groupId: {}, artifactId: {}, version: {}, reported_license: {}, registered_license: {}".format(mismatched_license[0], mismatched_license[1], mismatched_license[2], mismatched_license[3], mismatched_license[4], mismatched_license[5]))
        print_log_to_stderr("")

    # Let's find missing licenses, which are reported but missing in the registry.
    missing_licenses = find_missing_licenses(
        reported_dep_to_licenses, reported_modules, registered_modules, skipping_licenses, exclude_module_prefixes
    )

    if len(missing_licenses) > 0:
        print_log_to_stderr("Error: found {} missing licenses. These licenses are reported, but missing in the registry".format(len(missing_licenses)))
        for missing_license in missing_licenses:
            print_log_to_stderr("druid_module: {}, groupId: {}, artifactId: {}, version: {}, license: {}".format(missing_license[0], missing_license[1], missing_license[2], missing_license[3], missing_license[4]))
        print_log_to_stderr("")

    # Let's find unchecked licenses, which are registered but missing in the report.
    # These licenses should be checked manually.
    for key, registered_license in registered_dep_to_licenses.items():
        if key not in reported_dep_to_licenses:
            unchecked_licenses.append((key[0], key[1], key[2], registered_license))
        elif reported_dep_to_licenses[key][0] == "-":
            unchecked_licenses.append((key[0], key[1], key[2], registered_license))

    if len(unchecked_licenses) > 0:
        print_log_to_stderr("Warn: found {} unchecked licenses. These licenses are registered, but not found in dependency reports.".format(len(unchecked_licenses)))
        print_log_to_stderr("These licenses must be checked manually.")
        for unchecked_license in unchecked_licenses:
            print_log_to_stderr("groupId: {}, artifactId: {}, version: {}, reported_license: {}".format(unchecked_license[0], unchecked_license[1], unchecked_license[2], unchecked_license[3]))
    print_log_to_stderr("")

    if len(mismatched_licenses) > 0 or len(missing_licenses) > 0:
        print_log_to_stderr("Mismatched licenses: {}".format(mismatched_licenses))
        print_log_to_stderr("Missing licenses: {}".format(missing_licenses))
        sys.exit(1)


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description='Check and generate license file.')
        parser.add_argument('license_yaml', metavar='<path to license.yaml>', type=str)
        parser.add_argument('dependency_reports_root', metavar='<root to maven dependency reports>', type=str)
        parser.add_argument(
            '--exclude-module-prefix',
            action='append',
            default=[],
            help='Module prefix whose licenses.yaml entries the release LICENSE omits. Repeatable. A dependency '
                 'that ships in the release must be registered under at least one module not matching these.'
        )
        args = parser.parse_args()

        license_yaml = args.license_yaml
        dependency_reports_root = args.dependency_reports_root
        check_licenses(license_yaml, dependency_reports_root, args.exclude_module_prefix)

    except KeyboardInterrupt:
        print('Interrupted, closing.')
