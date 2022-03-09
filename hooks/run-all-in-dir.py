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
import sys
import subprocess


if len(sys.argv) < 2:
	sys.stderr.write('usage: program <hooks directory>\n')
	sys.exit(1)

hooks_dir = sys.argv[1]
args = sys.argv[2:]

for hook in os.listdir(hooks_dir):
	if not hook.startswith("_"):
		command = [os.path.join(hooks_dir, hook)] + args
		print("Running {}".format(command))
		subprocess.run(command, check=True)
