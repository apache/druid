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


class LicensesYamlLoader(yaml.SafeLoader):
    # Loads plain scalars that look like numbers as strings, so that versions such as 1.10 do not need to be
    # quoted in licenses.yaml. With the default resolvers, 1.10 would be loaded as the float 1.1.
    pass


LicensesYamlLoader.yaml_implicit_resolvers = {
    first_char: [(tag, regexp) for tag, regexp in resolvers if tag not in ('tag:yaml.org,2002:int', 'tag:yaml.org,2002:float')]
    for first_char, resolvers in yaml.SafeLoader.yaml_implicit_resolvers.items()
}


def load_all(stream):
    return list(yaml.load_all(stream, Loader=LicensesYamlLoader))
