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


outfile = sys.stdout


moduleHeaderLine = "############"
dependencyHeaderLine = "================="

def print_outfile(string):
    print(string, file=outfile)

def print_error(string):
    print(string, file=sys.stderr)

def print_jar(name, version, notice):
    print_outfile("{} {}-{}.jar {}".format(dependencyHeaderLine, name, version, dependencyHeaderLine))
    print_outfile("{}\n\n\n\n".format(notice))

def print_notice(dependency):
    if 'notice' in dependency:
        print_outfile("{} {} {} {}".format(dependencyHeaderLine, dependency['name'], dependency['version'], dependencyHeaderLine))
        if 'libraries' in dependency:
            for library in dependency['libraries']:
                for group_id, artifact_id in library.items():
                    print_outfile("{}.jar".format(artifact_id))
            print_outfile("{}\n".format(dependencyHeaderLine))
        print_outfile("{}\n\n\n\n".format(dependency['notice']))
    elif 'notices' in dependency:
        for notice_entry in dependency['notices']:
            for jar, notice in notice_entry.items():
                print_jar(jar, dependency['version'], notice)


def generate_notice(source_notice, dependences_yaml):
    # Generate NOTICE.BINARY file
    print_error("=== Generating the contents of NOTICE.BINARY file ===\n")

    # Print Apache license first.
    print_outfile(source_notice)
    with open(dependences_yaml) as registry_file:
        dependencies_list = list(yaml.load_all(registry_file))

    # Group dependencies by module
    modules_map = {}
    for dependency in dependencies_list:
        if 'notice' in dependency or 'notices' in dependency:
            if dependency['module'] not in modules_map:
                modules_map[dependency['module']] = []
            module_dependencies = modules_map[dependency['module']]
            module_dependencies.append(dependency)

    for module_name, dependencies_of_module in modules_map.items():
        print_outfile("{} BINARY/{} {}\n".format(moduleHeaderLine, module_name.upper(), moduleHeaderLine))
        for dependency in dependencies_of_module:
            print_notice(dependency)


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description='generate binary notice file.')
        parser.add_argument('notice', metavar='<path to apache notice file>', type=str)
        parser.add_argument('license_yaml', metavar='<path to license.yaml>', type=str)
        parser.add_argument('out_path', metavar='<path to output file>', type=str)
        args = parser.parse_args()

        with open(args.notice) as apache_notice_file:
            source_notice = apache_notice_file.read()
        dependencies_yaml = args.license_yaml

        with open(args.out_path, "w") as outfile:
            generate_notice(source_notice, dependencies_yaml)

    except KeyboardInterrupt:
        print('Interrupted, closing.')