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

existing_jar_dict_notice = {}

def main():
    if len(sys.argv) != 3:
      sys.stderr.write('usage: program <full extracted druid distribution path> <full tmp path>\n')
      sys.exit(1)

    druid_path = sys.argv[1]
    tmp_path = sys.argv[2]

    # copy everything in lib/ to the staging dir
    lib_path = druid_path + "/lib"
    tmp_lib_path = tmp_path + "/1-lib"
    os.mkdir(tmp_lib_path)
    command = "cp -r {}/* {}".format(lib_path, tmp_lib_path)
    subprocess.check_output(command, shell=True).decode('UTF-8')

    # copy hadoop deps to the staging dir
    hdeps_path = druid_path + "/hadoop-dependencies"
    tmp_hdeps_path = tmp_path + "/2-hdeps"
    os.mkdir(tmp_hdeps_path)
    command = "cp -r {}/* {}".format(hdeps_path, tmp_hdeps_path)
    subprocess.check_output(command, shell=True).decode('UTF-8')


    # copy all extension folders to the staging dir
    ext_path = druid_path + "/extensions"
    tmp_ext_path = tmp_path + "/3-ext"
    os.mkdir(tmp_ext_path)
    command = "cp -r {}/* {}".format(ext_path, tmp_ext_path)
    subprocess.check_output(command, shell=True).decode('UTF-8')


    get_notices(tmp_path)

def get_notices(tmp_jar_path):
    print("********** Scanning directory for NOTICE" + tmp_jar_path + " **********")
    jar_files = os.listdir(tmp_jar_path)
    os.chdir(tmp_jar_path)

    for jar_file in jar_files:
        if os.path.isdir(jar_file):
            get_notices(jar_file)
            continue
        elif not os.path.isfile(jar_file) or ".jar" not in jar_file:
            continue

        if existing_jar_dict_notice.get(jar_file) is not None:
            print("---------- Already saw file: " + jar_file)
            continue
        else:
            existing_jar_dict_notice[jar_file] = True

        try:
            command = "jar tf {} | grep NOTICE".format(jar_file)
            outstr = subprocess.check_output(command, shell=True).decode('UTF-8')
        except:
            print("---------- no NOTICE file found in: " + jar_file)
            continue

        for line in outstr.splitlines():
            try:
                command = "jar xf {} {}".format(jar_file, line)
                outstr = subprocess.check_output(command, shell=True).decode('UTF-8')

                command = "mv {} {}.NOTICE-FILE".format(line, jar_file)
                outstr = subprocess.check_output(command, shell=True).decode('UTF-8')

                command = "cat {}.NOTICE-FILE".format(jar_file)
                outstr = subprocess.check_output(command, shell=True).decode('UTF-8')
                print("================= " + jar_file + " =================")
                print(outstr)
                print("\n")
            except:
                print("Error while grabbing NOTICE file: " + jar_file)
                continue

    os.chdir("..")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted, closing.')