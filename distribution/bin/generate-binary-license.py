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

import yaml
import sys
import argparse

outfile = None

def print_outfile(string):
    print(string, file=outfile)

def print_log_to_stderr(string):
    print(string, file=sys.stderr)

def get_dep_key(group_id, artifact_id, version):
    return (group_id, artifact_id, version)

def get_version_string(version):
    if type(version) == str:
        return version
    else:
        return str(version)

def module_to_upper(module):
    extensions_offset = module.lower().find("extensions")
    if extensions_offset < 0:
        return module.upper()
    elif extensions_offset == 0:
        return module[0:len("extensions")].upper() + module[len("extensions"):len(module)]
    else:
        raise Exception("Expected extensions at 0, but {}".format(extensions_offset))

def is_non_empty(dic, key):
    if key in dic and dic[key] is not None:
        if type(dic[key]) == str:
            return len(dic[key]) > 0
        else:
            return True
    else:
        return False

def print_license_phrase(license_phrase):
    remaining = license_phrase
    while len(remaining) > 0:
        if len(remaining) > 120:
            chars_of_200 = remaining[0:120]
            phrase_len = chars_of_200.rfind(" ")
            if phrase_len < 0:
                raise Exception("Can't find whitespace in {}".format(chars_of_200))
            print_outfile("    {}".format(remaining[0:phrase_len]))
            remaining = remaining[phrase_len:]
        else:
            print_outfile("    {}".format(remaining))
            remaining = ""

def print_license(license):
    license_phrase = "This product"
    if license['license_category'] == "source":
        license_phrase += " contains"
    elif license['license_category'] == "binary":
        license_phrase += " bundles"
    license_phrase += " {}".format(license['name'])
    if is_non_empty(license, 'version'):
        license_phrase += " version {}".format(license['version'])
    if is_non_empty(license, 'copyright'):
        license_phrase += ", copyright {}".format(license['copyright'])
    if is_non_empty(license, 'additional_copyright_statement'):
        license_phrase += ", {}".format(license['additional_copyright_statement'])
    if license['license_name'] != 'Apache License version 2.0':
        license_phrase += " which is available under {}".format(license['license_name'])
    if is_non_empty(license, 'additional_license_statement'):
        license_phrase += ", {}".format(license['additional_license_statement'])
    if is_non_empty(license, 'license_file_path'):
        license_file_list = []
        if type(license['license_file_path']) == list:
            license_file_list.extend(license['license_file_path'])
        else:
            license_file_list.append(license['license_file_path'])
        if len(license_file_list) == 1:
            license_phrase += ". For details, see {}".format(license_file_list[0])
        else:
            license_phrase += ". For details, "
            for each_file in license_file_list:
                if each_file == license_file_list[-1]:
                    license_phrase += ", and {}".format(each_file)
                elif each_file == license_file_list[0]:
                    license_phrase += "see {}".format(each_file)
                else:
                    license_phrase += ", {}".format(each_file)
    
    license_phrase += "."

    print_license_phrase(license_phrase)

    if 'source_paths' in license:
        for source_path in license['source_paths']:
            if type(source_path) is dict:
                for class_name, path in source_path.items():
                    print_outfile("      {}:".format(class_name))
                    print_outfile("      * {}".format(path))
            else:
                print_outfile("      * {}".format(source_path))

    if 'libraries' in license:
        for library in license['libraries']:
            if type(library) is not dict:
                raise Exception("Expected dict but got {}[{}]".format(type(library), library))
            if len(library) > 1:
                raise Exception("Expected 1 groupId and artifactId, but got [{}]".format(library))
            for group_id, artifact_id in library.items():
                print_outfile("      * {}:{}".format(group_id, artifact_id))

def print_license_name_underbar(license_name):
    underbar = ""
    for _ in range(len(license_name)):
        underbar += "="
    print_outfile("{}\n".format(underbar))

def generate_license(apache_license_v2, license_yaml):
    print_log_to_stderr("=== Generating the contents of LICENSE.BINARY file ===\n")
    
    # Print Apache license first.
    print_outfile(apache_license_v2)
    with open(license_yaml, encoding='utf-8') as registry_file:
        licenses_list = list(yaml.load_all(registry_file))

    # Group licenses by license_name, license_category, and then module.
    licenses_map = {}
    for license in licenses_list:
        if license['license_name'] not in licenses_map:
            licenses_map[license['license_name']] = {}
        licenses_of_name = licenses_map[license['license_name']]
        if license['license_category'] not in licenses_of_name:
            licenses_of_name[license['license_category']] = {}
        licenses_of_category = licenses_of_name[license['license_category']]
        if license['module'] not in licenses_of_category:
            licenses_of_category[license['module']] = []
        licenses_of_module = licenses_of_category[license['module']]
        licenses_of_module.append(license)

    for license_name, licenses_of_name in sorted(licenses_map.items()):
        print_outfile(license_name)
        print_license_name_underbar(license_name)
        for license_category, licenses_of_category in licenses_of_name.items():
            for module, licenses in licenses_of_category.items():
                print_outfile("{}/{}".format(license_category.upper(), module_to_upper(module)))
                for license in licenses:
                    print_license(license)
                    print_outfile("")
                print_outfile("")


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description='Check and generate license file.')
        parser.add_argument('apache_license', metavar='<path to apache license file>', type=str)
        parser.add_argument('license_yaml', metavar='<path to license.yaml>', type=str)
        parser.add_argument('out_path', metavar='<path to output file>', type=str)
        args = parser.parse_args()
        
        with open(args.apache_license, encoding="ascii") as apache_license_file:
            apache_license_v2 = apache_license_file.read()
        license_yaml = args.license_yaml

        with open(args.out_path, "w", encoding="utf-8") as outfile:
            generate_license(apache_license_v2, license_yaml)

    except KeyboardInterrupt:
        print('Interrupted, closing.')
