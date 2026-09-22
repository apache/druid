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
import sys
import yaml

from collections import defaultdict

outfile = None

moduleHeaderLine = "#" * 12
dependencyHeaderLine = "=" * 17

def print_outfile(string):
    print(string, file=outfile)

def print_log_to_stderr(string):
    print(string, file=sys.stderr)

def normalize_module_for_output(module):
    # Core extensions are already tagged extensions/<output-dir-name> and need no normalization.
    # Contrib entries use the extensions-contrib/ prefix so the release build can filter them out,
    # but if a build DOES include contrib (via the bundle-contrib-exts profile, not distributed as
    # part of the Apache release), collapse the prefix to extensions/ so section headers match the
    # binary output directory layout (contrib extensions land alongside core in the same extensions/
    # dir when bundled).
    if module.startswith('extensions-contrib/'):
        return 'extensions/' + module[len('extensions-contrib/'):]
    return module

def print_notice(dependency):
    # note that a dependency may either supply a global notice in the 'notice' field, or, a per jar notice in the
    # 'notices' field
    if 'notice' in dependency:
        # single notice for dependency name, list out all 'libraries' if any, then print notice
        print_outfile("{} {} {} {}".format(dependencyHeaderLine, dependency['name'], dependency['version'], dependencyHeaderLine))
        if 'libraries' in dependency:
            for library in dependency['libraries']:
                for group_id, artifact_id in library.items():
                    print_outfile("{}.jar".format(artifact_id))
            print_outfile("{}".format(dependencyHeaderLine))
        print_outfile("{}\n\n\n\n".format(dependency['notice']))
    elif 'notices' in dependency:
        # if 'notices' is set instead of 'notice', then it has jar specific notices to print
        for notice_entry in dependency['notices']:
            for jar, notice in notice_entry.items():
                print_outfile("{} {}-{}.jar {}".format(dependencyHeaderLine, jar, dependency['version'], dependencyHeaderLine))
                print_outfile("{}\n\n\n\n".format(notice))

def generate_notice(source_notice, dependences_yaml, exclude_module_prefixes):
    print_log_to_stderr("=== Generating the contents of NOTICE.BINARY file ===\n")

    # Print Apache license first.
    print_outfile(source_notice)
    with open(dependences_yaml, encoding='utf-8') as registry_file:
        dependencies = list(yaml.load_all(registry_file, Loader=yaml.Loader))

    # Filter out entries whose module matches any of the excluded prefixes. Used by the release build
    # to skip contrib extension entries (module: extensions-contrib/*) since contrib extensions are not
    # bundled in the Apache release binary. check-licenses.py validates that every entry uses a known
    # module prefix, so this filter can rely on contrib entries always being tagged extensions-contrib/*
    # rather than smuggled in under some other prefix.
    if exclude_module_prefixes:
        filtered = []
        for dependency in dependencies:
            if any(dependency.get('module', '').startswith(prefix) for prefix in exclude_module_prefixes):
                print_log_to_stderr("Excluding notice entry for module [{}] (matched exclude prefix)".format(dependency.get('module')))
            else:
                filtered.append(dependency)
        dependencies = filtered

    # Group dependencies by module
    modules_map = defaultdict(list)
    for dependency in dependencies:
        if 'notice' in dependency or 'notices' in dependency:
            modules_map[dependency['module']].append(dependency)

    # print notice(s) of dependencies by module
    for module_name, dependencies_of_module in modules_map.items():
        print_outfile("{} BINARY/{} {}\n".format(moduleHeaderLine, normalize_module_for_output(module_name).upper(), moduleHeaderLine))
        for dependency in dependencies_of_module:
            print_notice(dependency)


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description='generate binary notice file.')
        parser.add_argument('notice', metavar='<path to apache notice file>', type=str)
        parser.add_argument('license_yaml', metavar='<path to license.yaml>', type=str)
        parser.add_argument('out_path', metavar='<path to output file>', type=str)
        parser.add_argument(
            '--exclude-module-prefix',
            action='append',
            default=[],
            help='Skip licenses.yaml entries whose module starts with this prefix. Repeatable. Used by '
                 'the release build to exclude contrib extension entries '
                 '(--exclude-module-prefix=extensions-contrib/) since contrib extensions are not '
                 'bundled in the Apache release binary. Omit to disable filtering (include everything).'
        )
        args = parser.parse_args()

        with open(args.notice, encoding="ascii") as apache_notice_file:
            source_notice = apache_notice_file.read()
        dependencies_yaml = args.license_yaml

        with open(args.out_path, "w", encoding="utf-8") as outfile:
            generate_notice(source_notice, dependencies_yaml, args.exclude_module_prefix)

    except KeyboardInterrupt:
        print('Interrupted, closing.')
