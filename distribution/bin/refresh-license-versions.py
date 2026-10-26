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

import argparse
import importlib.util
import os
import re
import sys

import yaml

spec = importlib.util.spec_from_file_location(
    "check_licenses", os.path.join(os.path.dirname(os.path.abspath(__file__)), "check-licenses.py")
)
check_licenses = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check_licenses)

VERSION_LINE = re.compile(r"^version:[ \t]*(.*?)[ \t]*(\r?\n)?$")


def split_documents(text):
    # Returns the licenses.yaml lines grouped into YAML documents; each group starts after a '---' line.
    lines = text.splitlines(keepends=True)
    documents = [[]]
    for line in lines:
        documents[-1].append(line)
        if line.strip() == "---":
            documents.append([])
    return documents


def find_version_line(lines):
    for index, line in enumerate(lines):
        match = VERSION_LINE.match(line)
        if match:
            return index, match.group(1)
    return None, None


def unquote(raw):
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in "\"'":
        return raw[1:-1], raw[0]
    return raw, None


def format_version(version, quote):
    # Quote when the old value was quoted, or when YAML would not read the new value back as a string (2.22 is a float).
    if quote is None and not isinstance(yaml.safe_load(version), str):
        quote = '"'
    return version if quote is None else quote + version + quote


def index_reports(reported_modules):
    # (group_id, artifact_id, version) -> modules becomes (group_id, artifact_id) -> module -> versions.
    versions = {}
    for (group_id, artifact_id, version), modules in reported_modules.items():
        for module in modules:
            versions.setdefault((group_id, artifact_id), {}).setdefault(module, set()).add(version)
    return versions


def refresh_versions(text, reported_modules):
    # Updates the version of every licenses.yaml entry whose libraries all moved to one new reported version.
    # Returns the new text, the updated entries as (name, module, old, new) and the entries that need a person,
    # as (name, module, reason). Only version lines change, so comments and notices stay as they are.
    reported = index_reports(reported_modules)
    report_modules = {module for modules in reported_modules.values() for module in modules}
    documents = split_documents(text)

    entries = []
    for lines in documents:
        body = lines[:-1] if lines and lines[-1].strip() == "---" else lines
        entry = yaml.safe_load("".join(body))
        line_index, raw = find_version_line(lines)
        if not isinstance(entry, dict) or "libraries" not in entry or line_index is None or not raw:
            continue
        version, quote = unquote(raw)
        libraries = [library for item in entry["libraries"] for library in item.items()]
        entries.append((lines, line_index, entry, version, quote, libraries))

    # A reported version that another entry already registers belongs to that entry, not to a stale one.
    registered = {}
    for _, _, entry, version, _, libraries in entries:
        for library in libraries:
            registered.setdefault(library, {}).setdefault(entry.get("module"), set()).add(version)

    proposals = []
    flagged = []
    for lines, line_index, entry, version, quote, libraries in entries:
        name, module = entry.get("name"), entry.get("module")
        if entry.get("skip_dependency_report_check") or module not in report_modules:
            continue

        current = []
        unreported = []
        moved_to = set()
        problems = []
        for library in libraries:
            by_module = reported.get(library, {})
            all_versions = set().union(*by_module.values()) if by_module else set()
            if version in all_versions:
                current.append(library)
                continue
            if not all_versions:
                unreported.append("{}:{}".format(*library))
                continue
            # Prefer the entry's own module report; only entries of that module compete for its versions.
            if module in by_module:
                pool, taken = by_module[module], registered[library].get(module, set())
            else:
                pool, taken = all_versions, set().union(*registered[library].values())
            candidates = pool - (taken - {version})
            if len(candidates) == 1:
                moved_to.update(candidates)
            elif candidates:
                problems.append("{}:{} has several new versions {}".format(*library, sorted(candidates)))
            # Otherwise every reported version belongs to another entry; check-licenses lists this one as unchecked.

        if not problems and len(moved_to) > 1:
            problems.append("libraries moved to different versions {}; split the entry".format(sorted(moved_to)))
        if not problems and moved_to and current:
            problems.append("some libraries are still at {}; split the entry".format(version))
        if not problems and moved_to and unreported:
            problems.append("{} are in no report; update or remove them by hand".format(", ".join(unreported)))
        if problems:
            flagged.append((name, module, "; ".join(problems)))
        elif moved_to:
            proposals.append((lines, line_index, name, module, version, quote, libraries, moved_to.pop()))

    # Two stale entries of one module moving the same library to the same version would register it twice.
    claimants = {}
    for index, (_, _, _, module, _, _, libraries, new_version) in enumerate(proposals):
        for library in libraries:
            claimants.setdefault((module, library, new_version), set()).add(index)

    updated = []
    for index, (lines, line_index, name, module, version, quote, libraries, new_version) in enumerate(proposals):
        rivals = set().union(*(claimants[(module, library, new_version)] for library in libraries)) - {index}
        if rivals:
            rival_names = sorted({proposals[rival][2] for rival in rivals})
            flagged.append((name, module, "entries {} move the same libraries to {}; keep one".format(rival_names, new_version)))
            continue
        newline = VERSION_LINE.match(lines[line_index]).group(2) or ""
        lines[line_index] = "version: {}{}".format(format_version(new_version, quote), newline)
        updated.append((name, module, version, new_version))

    return "".join(line for lines in documents for line in lines), updated, flagged


def main():
    parser = argparse.ArgumentParser(description="Refresh the versions in licenses.yaml from the Maven dependency reports.")
    parser.add_argument("license_yaml", metavar="<path to licenses.yaml>", type=str)
    parser.add_argument("dependency_reports_root", metavar="<root to maven dependency reports>", type=str)
    parser.add_argument("--dry-run", action="store_true", help="Print the changes without writing licenses.yaml.")
    args = parser.parse_args()

    _, reported_modules = check_licenses.read_dependency_reports(
        args.dependency_reports_root, check_licenses.build_compatible_license_names()
    )
    if not reported_modules:
        raise Exception("No dependency reports are found")

    with open(args.license_yaml, encoding="utf-8", newline="") as f:
        text = f.read()
    new_text, updated, flagged = refresh_versions(text, reported_modules)

    for name, module, old, new in updated:
        print("updated: {} ({}): {} -> {}".format(name, module, old, new))
    for name, module, reason in flagged:
        print("needs a manual edit: {} ({}): {}".format(name, module, reason))
    print("{} entries updated, {} need a manual edit".format(len(updated), len(flagged)))

    if updated and not args.dry_run:
        with open(args.license_yaml, "w", encoding="utf-8", newline="") as f:
            f.write(new_text)
    if flagged:
        sys.exit(1)


if __name__ == "__main__":
    main()
