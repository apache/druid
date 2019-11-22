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
import argparse
import concurrent.futures


def generate_report(module_path, report_orig_path, report_out_path):
    is_dir = os.path.isdir(module_path)
    if not is_dir:
        print("{} is not a directory".format(module_path))
        return

    os.makedirs(report_out_path, exist_ok=True)

    try:
        print("Generating report for {}".format(module_path))
        # This command prints lots of false errors. Here, we redirect stdout and stderr to avoid them.
        command = "mvn -Ddependency.locations.enabled=false -Ddependency.details.enabled=false project-info-reports:dependencies"
        subprocess.check_output(command, cwd=module_path, shell=True)
        command = "cp -r {} {}".format(report_orig_path, report_out_path)
        subprocess.check_output(command, cwd=module_path, shell=True)
        print("Generated report for {} in {}".format(module_path, report_out_path))
    except Exception as e:
        print("Encountered error [{}] when generating report for {}".format(e, module_path))


def generate_reports(druid_path, tmp_path, exclude_ext, num_threads):
    tmp_path = os.path.abspath(tmp_path)
    license_report_root = os.path.join(tmp_path, "license-reports")
    license_core_path = os.path.join(license_report_root, "core")
    license_ext_path = os.path.join(license_report_root, "ext")
    shutil.rmtree(license_report_root, ignore_errors=True)
    os.makedirs(license_core_path)
    os.makedirs(license_ext_path)
    druid_path = os.path.abspath(druid_path)

    script_args = [(druid_path, os.path.join(druid_path, "distribution", "target", "site"), license_core_path)]

    if not exclude_ext:
        extensions_core_path = os.path.join(druid_path, "extensions-core")
        extension_dirs = os.listdir(extensions_core_path)
        print("Found {} extensions".format(len(extension_dirs)))
        for extension_dir in extension_dirs:
            print("extension dir: {}".format(extension_dir))
            extension_path = os.path.join(extensions_core_path, extension_dir)
            if not os.path.isdir(extension_path):
                print("{} is not a directory".format(extension_path))
                continue

            extension_report_dir = "{}/{}".format(license_ext_path, extension_dir)
            script_args.append((extension_path, os.path.join(extension_path, "target", "site"), extension_report_dir))

    print("Generating dependency reports")

    if num_threads > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            for module_path, report_orig_path, report_out_path in script_args:
                executor.submit(generate_report, module_path, report_orig_path, report_out_path)
    else:
        for module_path, report_orig_path, report_out_path in script_args:
            generate_report(module_path, report_orig_path, report_out_path)


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description='Generating dependency reports.')
        parser.add_argument('druid_path', metavar='<Druid source path>', type=str)
        parser.add_argument('tmp_path', metavar='<Full tmp path>', type=str)
        parser.add_argument('--exclude-extension', dest='exclude_ext', action='store_const', const=True, default=False, help="Exclude extension report")
        parser.add_argument('--clean-maven-artifact-transfer', dest='clean_mvn_artifact_transfer', action='store_const', const=True, default=False, help="Clean maven-artifact-transfer before generating dependency reports")
        parser.add_argument('--parallel', dest='num_threads', type=int, default=1, help='Number of threads for generating reports')
        args = parser.parse_args()

        # The default maven-artifact-transfer in Travis is currently corrupted. Set the below argument properly to remove the corrupted one.
        if args.clean_mvn_artifact_transfer:
            command = "rm -rf ~/.m2/repository/org/apache/maven/shared/maven-artifact-transfer"
            subprocess.check_call(command, shell=True)

        generate_reports(args.druid_path, args.tmp_path, args.exclude_ext, args.num_threads)
    except KeyboardInterrupt:
        print('Interrupted, closing.')
