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
import argparse


existing_jar_dict_notice = {}

def generate_reports(druid_path, tmp_path, exclude_ext):
    license_main_path =  tmp_path + "/license-reports"
    license_ext_path = tmp_path + "/license-reports/ext"
    os.mkdir(license_main_path)
    os.mkdir(license_ext_path)

    print("********** Generating main LICENSE report.... **********")
    os.chdir(druid_path)
    command = "mvn -Pdist -Ddependency.locations.enabled=false project-info-reports:dependencies"
    outstr = subprocess.check_output(command, shell=True).decode('UTF-8')
    command = "cp -r distribution/target/site {}/site".format(license_main_path)
    outstr = subprocess.check_output(command, shell=True).decode('UTF-8')

    if exclude_ext:
        sys.exit()

    print("********** Generating extension LICENSE reports.... **********")
    extension_dirs = os.listdir("extensions-core")
    print("Found {}".format(extension_dirs))
    for extension_dir in extension_dirs:
        full_extension_dir = druid_path + "/extensions-core/" + extension_dir
        if not os.path.isdir(full_extension_dir):
            print("{} is not a directory".format(full_extension_dir))
            continue

        print("--- Generating report for {}... ---".format(extension_dir))

        extension_report_dir = "{}/{}".format(license_ext_path, extension_dir)
        os.mkdir(extension_report_dir)
        prev_work_dir = os.getcwd()
        os.chdir(full_extension_dir)

        try:
            command = "mvn -Ddependency.locations.enabled=false project-info-reports:dependencies"
            outstr = subprocess.check_output(command, shell=True).decode('UTF-8')
            command = "cp -r target/site {}/site".format(extension_report_dir)
            outstr = subprocess.check_output(command, shell=True).decode('UTF-8')
        except:
            print("Encountered error when generating report for: " + extension_dir)

        os.chdir(prev_work_dir)

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description='Generating dependency reports.')
        parser.add_argument('druid_path', metavar='<Druid source path>', type=str)
        parser.add_argument('tmp_path', metavar='<Full tmp path>', type=str)
        parser.add_argument('--exclude-extension', dest='exclude_ext', action='store_const', const=True, default=False, help="Exclude extension report")
        args = parser.parse_args()
        generate_reports(args.druid_path, args.tmp_path, args.exclude_ext)
    except KeyboardInterrupt:
        print('Interrupted, closing.')
