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
const snarkdown = require('snarkdown');

const writefile = 'lib/sql-docs.js';

const MINIMUM_EXPECTED_NUMBER_OF_FUNCTIONS = 167;
const MINIMUM_EXPECTED_NUMBER_OF_DATA_TYPES = 14;

const initialFunctionDocs = {
  TABLE: [['external', convertMarkdownToHtml('Defines a logical table from an external.')]],
  EXTERN: [
    ['inputSource, inputFormat, rowSignature?', convertMarkdownToHtml('Reads external data.')],
  ],
  TYPE: [
    [
      'nativeType',
      convertMarkdownToHtml(
        'A purely type system modification function what wraps a Druid native type to make it into a SQL type.',
      ),
    ],
  ],
};

function hasHtmlTags(str) {
  return /<(a|br|span|div|p|code)\/?>/.test(str);
}

function sanitizeArguments(str) {
  str = str.replace(/`<code>&#124;<\/code>`/g, '|'); // convert the hack to get | in a table to a normal pipe

  // Ensure there are no more html tags other than the <code> we just removed
  if (hasHtmlTags(str)) {
    throw new Error(`Arguments contain HTML: ${str}`);
  }

  return str;
}

function convertMarkdownToHtml(markdown) {
  markdown = markdown.replace(/<br\/?>/g, '\n'); // Convert inline <br> to newlines

  // Ensure there are no more html tags other than the <br> we just removed
  if (hasHtmlTags(markdown)) {
    throw new Error(`Markdown contains HTML: ${markdown}`);
  }

  // Concert to markdown
  markdown = snarkdown(markdown);

  return markdown.replace(/<a[^>]*>(.*?)<\/a>/g, '$1'); // Remove links
}

const readDoc = async () => {
  const data = [
    await fs.readFile('../docs/querying/sql-data-types.md', 'utf-8'),
    await fs.readFile('../docs/querying/sql-scalar.md', 'utf-8'),
    await fs.readFile('../docs/querying/sql-aggregations.md', 'utf-8'),
    await fs.readFile('../docs/querying/sql-array-functions.md', 'utf-8'),
    await fs.readFile('../docs/querying/sql-multivalue-string-functions.md', 'utf-8'),
    await fs.readFile('../docs/querying/sql-json-functions.md', 'utf-8'),
    await fs.readFile('../docs/querying/sql-operators.md', 'utf-8'),
  ].join('\n');

  const lines = data.split('\n');

  const functionDocs = initialFunctionDocs;
  const dataTypeDocs = {};
  for (let line of lines) {
    const functionMatch = line.match(/^\|\s*`(\w+)\(([^|]*)\)`\s*\|([^|]+)\|(?:([^|]+)\|)?$/);
    if (functionMatch) {
      const functionName = functionMatch[1];
      const args = sanitizeArguments(functionMatch[2]);
      const description = convertMarkdownToHtml(functionMatch[3]);

      functionDocs[functionName] = functionDocs[functionName] || [];
      functionDocs[functionName].push([args, description]);
    }

    const dataTypeMatch = line.match(/^\|([A-Z]+)\|([A-Z]+)\|([^|]*)\|([^|]*)\|$/);
    if (dataTypeMatch) {
      dataTypeDocs[dataTypeMatch[1]] = [dataTypeMatch[2], convertMarkdownToHtml(dataTypeMatch[4])];
    }
  }

  // Make sure there are enough functions found
  const numFunction = Object.keys(functionDocs).length;
  if (!(MINIMUM_EXPECTED_NUMBER_OF_FUNCTIONS <= numFunction)) {
    throw new Error(
      `Did not find enough function entries did the structure of '${readfile}' change? (found ${numFunction} but expected at least ${MINIMUM_EXPECTED_NUMBER_OF_FUNCTIONS})`,
    );
  }

  // Make sure there are at least 10 data types for sanity
  const numDataTypes = Object.keys(dataTypeDocs).length;
  if (!(MINIMUM_EXPECTED_NUMBER_OF_DATA_TYPES <= numDataTypes)) {
    throw new Error(
      `Did not find enough data type entries did the structure of '${readfile}' change? (found ${numDataTypes} but expected at least ${MINIMUM_EXPECTED_NUMBER_OF_DATA_TYPES})`,
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

  console.log(`Found ${numDataTypes} data types and ${numFunction} functions`);
  await fs.writeFile(writefile, content, 'utf-8');
};

readDoc();
