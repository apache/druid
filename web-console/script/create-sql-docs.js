#!/usr/bin/env node

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const fs = require('fs-extra');

const readfile = '../docs/querying/sql.md';
const writefile = 'lib/sql-docs.js';

const MINIMUM_EXPECTED_NUMBER_OF_FUNCTIONS = 152;
const MINIMUM_EXPECTED_NUMBER_OF_DATA_TYPES = 14;

function unwrapMarkdownLinks(str) {
  return str.replace(/\[([^\]]+)\]\([^)]+\)/g, (_, s) => s);
}

function deleteBackticks(str) {
  return str.replace(/`/g, "");
}

const readDoc = async () => {
  const data = await fs.readFile(readfile, 'utf-8');
  const lines = data.split('\n');

  const functionDocs = [];
  const dataTypeDocs = [];
  for (let line of lines) {
    const functionMatch = line.match(/^\|\s*`(\w+)\(([^|]*)\)`\s*\|([^|]+)\|(?:([^|]+)\|)?$/);
    if (functionMatch) {
      functionDocs.push([
        functionMatch[1],
        deleteBackticks(functionMatch[2]),
        deleteBackticks(unwrapMarkdownLinks(functionMatch[3])),
        // functionMatch[4] would be the default column but we ignore it for now
      ]);
    }

    const dataTypeMatch = line.match(/^\|([A-Z]+)\|([A-Z]+)\|([^|]*)\|([^|]*)\|$/);
    if (dataTypeMatch) {
      dataTypeDocs.push([
        dataTypeMatch[1],
        dataTypeMatch[2],
        unwrapMarkdownLinks(dataTypeMatch[4]),
      ]);
    }
  }

  // Make sure there are enough functions found
  if (functionDocs.length < MINIMUM_EXPECTED_NUMBER_OF_FUNCTIONS) {
    throw new Error(
      `Did not find enough function entries did the structure of '${readfile}' change? (found ${functionDocs.length} but expected at least ${MINIMUM_EXPECTED_NUMBER_OF_FUNCTIONS})`,
    );
  }

  // Make sure there are at least 10 data types for sanity
  if (dataTypeDocs.length < MINIMUM_EXPECTED_NUMBER_OF_DATA_TYPES) {
    throw new Error(
      `Did not find enough data type entries did the structure of '${readfile}' change? (found ${dataTypeDocs.length} but expected at least ${MINIMUM_EXPECTED_NUMBER_OF_DATA_TYPES})`,
    );
  }

  const content = `/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// This file is auto generated and should not be modified

// prettier-ignore
exports.SQL_DATA_TYPES = ${JSON.stringify(dataTypeDocs, null, 2)};

// prettier-ignore
exports.SQL_FUNCTIONS = ${JSON.stringify(functionDocs, null, 2)};
`;

  console.log(`Found ${dataTypeDocs.length} data types and ${functionDocs.length} functions`);
  await fs.writeFile(writefile, content, 'utf-8');
};

readDoc();
