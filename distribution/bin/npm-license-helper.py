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

import json
import re
import shutil
import sys

# Helper program for generating LICENSE contents for dependencies under web-console.
# Generates entries for MIT-licensed deps and dumps info for non-MIT deps.
# Uses JSON output from https://www.npmjs.com/package/license-checker.

if len(sys.argv) != 3:
  sys.stderr.write('usage: program <license-report-path> <license-output-path>\n')
  sys.stderr.write('Run the following command in web-console/ to generate the input license report:\n')
  sys.stderr.write('  license-checker --production --json\n')
  sys.exit(1)

license_report_path = sys.argv[1]
license_output_path = sys.argv[2]

non_mit_licenses = []

license_entry_template = "This product bundles {} version {}, copyright {},\n  which is available under an MIT license. For details, see licenses/{}.MIT.\n"

with open(license_report_path, 'r') as license_report_file:
  license_report = json.load(license_report_file)
  for dependency_name_version in license_report:
    dependency = license_report[dependency_name_version]

    match_result = re.match("(.+)@(.+)", dependency_name_version)
    dependency_name = match_result.group(1)
    nice_dependency_name = dependency_name.replace("/", "-")
    dependency_ver = match_result.group(2)

    try:
      licenseType = dependency["licenses"]
      licenseFile = dependency["licenseFile"]
    except:
      print("No license file for {}".format(dependency_name_version))

    try:
      publisher = dependency["publisher"]
    except:
      publisher = ""

    if licenseType != "MIT":
      non_mit_licenses.append(dependency)
      continue

    fullDependencyPath = dependency["path"]
    partialDependencyPath = re.match(".*/(web-console.*)", fullDependencyPath).group(1)

    print(license_entry_template.format(dependency_name, dependency_ver, publisher, nice_dependency_name))
    shutil.copy2(licenseFile, license_output_path + "/" + nice_dependency_name + ".MIT")

  print("\nNon-MIT licenses:\n--------------------\n")
  for non_mit_license in non_mit_licenses:
  	print(non_mit_license)
