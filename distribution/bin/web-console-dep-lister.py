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

import re
import sys

# Helper program for listing the deps in the compiled web-console-<VERSION>.js file in druid-console.jar

if len(sys.argv) != 2:
  sys.stderr.write('usage: program <web-console js path>\n')
  sys.exit(1)

web_console_path = sys.argv[1]

dep_dict = {}
with open(web_console_path, 'r') as web_console_file:
    for line in web_console_file.readlines():
      match_result = re.match('/\*\*\*/ "\./node_modules/([\@\-a-zA-Z0-9_]+)/.*', line)
      if match_result != None:
        dependency_name = match_result.group(1)
        dep_dict[dependency_name] = True
    for dep in dep_dict:
        print(dep)