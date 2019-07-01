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
import shutil
import subprocess
import sys
import argparse
import concurrent.futures


def generate_report(module_path, report_path):
    if not os.path.isdir(module_path):
        print("{} is not a directory".format(module_path))
        return

    os.makedirs(report_path)

    try:
        command = "mvn -Pdist -Ddependency.locations.enabled=false project-info-reports:dependencies"
        subprocess.run(command, cwd=module_path, shell=True)
        command = "cp -r target/site {}/site".format(report_path)
        subprocess.run(command, cwd=module_path, shell=True)
    except Exception as e:
        print("Encountered error [{}] when generating report for {}".format(e, module_path))


def generate_reports(druid_path, tmp_path, exclude_ext, num_threads):
    license_report_root = os.path.join(tmp_path, "license-reports")
    license_core_path =  os.path.join(license_report_root, "core")
    license_ext_path = os.path.join(license_report_root, "ext")
    shutil.rmtree(license_report_root)
    os.makedirs(license_core_path)
    os.makedirs(license_ext_path)
    druid_path = os.path.abspath(druid_path)

    script_args = [(druid_path, license_core_path)]

    extensions_core_path = os.path.join(druid_path, "extensions-core")
    extension_dirs = os.listdir(extensions_core_path)
    print("Found {} extensions".format(len(extension_dirs)))
    for extension_dir in extension_dirs:
        extension_path = os.path.join(extensions_core_path, extension_dir)
        if not os.path.isdir(extension_path):
            print("{} is not a directory".format(extension_path))
            continue

        extension_report_dir = "{}/{}".format(license_ext_path, extension_dir)
        script_args.append((extension_path, extension_report_dir))

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        for module_path, report_path in script_args:
            executor.submit(generate_report, module_path, report_path)

    # print("********** Generating main LICENSE report.... **********")
    # os.chdir(druid_path)
    # command = "mvn -Pdist -Ddependency.locations.enabled=false project-info-reports:dependencies"
    # outstr = subprocess.check_output(command, shell=True).decode('UTF-8')
    # command = "cp -r distribution/target/site {}/site".format(license_main_path)
    # outstr = subprocess.check_output(command, shell=True).decode('UTF-8')

    # if exclude_ext:
    #     sys.exit()

    # print("********** Generating extension LICENSE reports.... **********")
    # extension_dirs = os.listdir("extensions-core")
    # print("Found {}".format(extension_dirs))
    # for extension_dir in extension_dirs:
    #     full_extension_dir = druid_path + "/extensions-core/" + extension_dir
    #     if not os.path.isdir(full_extension_dir):
    #         print("{} is not a directory".format(full_extension_dir))
    #         continue

    #     print("--- Generating report for {}... ---".format(extension_dir))

    #     extension_report_dir = "{}/{}".format(license_ext_path, extension_dir)
    #     os.mkdir(extension_report_dir)
    #     prev_work_dir = os.getcwd()
    #     os.chdir(full_extension_dir)

    #     try:
    #         command = "mvn -Ddependency.locations.enabled=false project-info-reports:dependencies"
    #         outstr = subprocess.check_output(command, shell=True).decode('UTF-8')
    #         command = "cp -r target/site {}/site".format(extension_report_dir)
    #         outstr = subprocess.check_output(command, shell=True).decode('UTF-8')
    #     except:
    #         print("Encountered error when generating report for: " + extension_dir)

    #     os.chdir(prev_work_dir)

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description='Generating dependency reports.')
        parser.add_argument('druid_path', metavar='<Druid source path>', type=str)
        parser.add_argument('tmp_path', metavar='<Full tmp path>', type=str)
        parser.add_argument('--exclude-extension', dest='exclude_ext', action='store_const', const=True, default=False, help="Exclude extension report")
        parser.add_argument('--parallel', dest='num_threads', type=int, default=1, help='Number of threads for generating reports')
        args = parser.parse_args()
        generate_reports(args.druid_path, args.tmp_path, args.exclude_ext, args.num_threads)
    except KeyboardInterrupt:
        print('Interrupted, closing.')
