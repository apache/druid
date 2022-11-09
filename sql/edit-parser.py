#! /bin/python3
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
# ------------------------------------------------------------------------

# Revise the parser to add two elements of Druid syntax to the FROM
# clause:
#
# id [ (<args>) ]
#
# And
#
# TABLE(<fn>(<args>)) (<schema>)

import os
import os.path

# Ensure this can run from the main pom, or within the
# module directory.
baseDir = ""
if os.path.isdir("sql"):
  baseDir = "sql/"
source = baseDir + "target/codegen/templates/Parser.jj"
dest = baseDir + "target/codegen/templates/DruidParser.jj"

inFile = open(source)
outFile = open(dest, "w")

# Look for the rule to remove, copying lines as we go.
while True:
    line = inFile.readline()
    if not line:
        break
    outFile.write(line)
    if line == "SqlNode TableRef2(boolean lateral) :\n":
        break

# Find close of the rule, after the variable definitions
while True:
    line = inFile.readline()
    if not line:
        break
    if line == "}\n":
        break
    outFile.write(line)

outFile.write(
'''    List<SqlNode> paramList;
}
''')

# Find the table identifier rule
while True:
    line = inFile.readline()
    if not line:
        break
    outFile.write(line)
    if line == "        tableRef = CompoundIdentifier()\n":
        break

# Add the Druid parameterization
outFile.write(
'''        [
            paramList = FunctionParameterList(ExprContext.ACCEPT_NONCURSOR)
            {
                tableRef = ParameterizeOperator.PARAM.createCall(tableRef, paramList);
            }
        ]
''')

# Skip over the unwanted EXTENDS clause
while True:
    line = inFile.readline()
    if not line:
        break
    if line == "        over = TableOverOpt() {\n":
        outFile.write(line)
        break

# Find the table function rule
while True:
    line = inFile.readline()
    if not line:
        break
    outFile.write(line)
    if line == "        tableRef = TableFunctionCall(s.pos())\n":
        break

# Find the closing paren
while True:
    line = inFile.readline()
    if not line:
        break
    outFile.write(line)
    if line == "        <RPAREN>\n":
        break

# Add the additional clause
outFile.write(
'''        [
            [ <EXTEND> ]
            extendList = ExtendList()
            {
                tableRef = ExtendOperator.EXTEND.createCall(
                        Span.of(tableRef, extendList).pos(), tableRef, extendList);
            }
        ]
''')

# Copy everything else
while True:
    line = inFile.readline()
    if not line:
        break
    outFile.write(line)

inFile.close()
outFile.close()

# Switch the files.
os.remove(source)
os.rename(dest, source)
