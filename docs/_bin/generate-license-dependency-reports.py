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
import time
import threading





def generate_report(module_path, report_orig_path, report_out_path):
    if not os.path.isdir(module_path):
        print("{} is not a directory".format(module_path))
        return

    os.makedirs(report_out_path, exist_ok=True)

    try:
        # This command prints lots of false errors. Here, we redirect stdout and stderr to avoid them.
        command = "mvn -Ddependency.locations.enabled=false project-info-reports:dependencies"
        subprocess.run(command, cwd=module_path, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, shell=True)
        command = "cp -r {} {}".format(report_orig_path, report_out_path)
        subprocess.run(command, cwd=module_path, check=True, shell=True)
    except Exception as e:
        print("Encountered error [{}] when generating report for {}".format(e, module_path))


def generate_reports(druid_path, tmp_path, exclude_ext, num_threads):
    tmp_path = os.path.abspath(tmp_path)
    license_report_root = os.path.join(tmp_path, "license-reports")
    license_core_path =  os.path.join(license_report_root, "core")
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
            extension_path = os.path.join(extensions_core_path, extension_dir)
            if not os.path.isdir(extension_path):
                print("{} is not a directory".format(extension_path))
                continue

            extension_report_dir = "{}/{}".format(license_ext_path, extension_dir)
            script_args.append((extension_path, os.path.join(extension_path, "target", "site"), extension_report_dir))
    
    print("Generating dependency reports", end="")
    running = True
    def print_dots():
        while running:
            print(".", end="", flush=True)
            time.sleep(10)
    dot_thread = threading.Thread(target=print_dots)
    dot_thread.start()

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        for module_path, report_orig_path, report_out_path in script_args:
            executor.submit(generate_report, module_path, report_orig_path, report_out_path)

    running = False
    dot_thread.join()


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
