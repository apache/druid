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

import fs from 'node:fs/promises';

const INPUT_FILELIST_FILE = 'script/sql-doc-files.txt';
const OUTPUT_FILE = 'lib/sql-docs.ts';

const MINIMUM_EXPECTED_NUMBER_OF_FUNCTIONS = 198;
const MINIMUM_EXPECTED_NUMBER_OF_DATA_TYPES = 15;

const HTML_ENTITIES = {
  '&mdash;': '\u2014',
  '&ndash;': '\u2013',
  '&nbsp;': ' ',
  '&#124;': '|',
  '&lt;': '<',
  '&gt;': '>',
  '&quot;': '"',
  '&amp;': '&',
};

// Emphasis: _text_ or *text*, not inside of words
const EMPHASIS_REGEXP = /(?<![\w*])([*_])(?!\s)(.+?)(?<!\s)\1(?![\w*])/g;

const initialFunctionDocs = {
  TABLE: ['external', sanitizeMarkdown('Defines a logical table from an external.')],
  EXTERN: ['inputSource, inputFormat, rowSignature?', sanitizeMarkdown('Reads external data.')],
  TYPE: [
    'nativeType',
    sanitizeMarkdown(
      'A purely type system modification function what wraps a Druid native type to make it into a SQL type.',
    ),
  ],
  UNNEST: [
    'arrayExpression',
    sanitizeMarkdown(
      "Unnests ARRAY typed values. The source for UNNEST can be an array type column, or an input that's been transformed into an array, such as with helper functions like `MV_TO_ARRAY` or `ARRAY`.",
    ),
  ],
};

function hasHtmlTags(str) {
  return /<\/?[a-z][^>]*>/i.test(str);
}

function sanitizeArguments(str) {
  str = str.replace(/`<code>&#124;<\/code>`/g, '|'); // convert the hack to get | in a table to a normal pipe

  // Ensure there are no more html tags other than the <code> we just removed
  if (hasHtmlTags(str)) {
    throw new Error(`Arguments contain HTML: ${str}`);
  }

  return str;
}

function escapeDocMarkdown(text) {
  return text.replace(/[\\*`]/g, '\\$&');
}

/**
 * Converts the text (not code) parts of a line
 */
function sanitizeText(text, context) {
  if (hasHtmlTags(text)) {
    throw new Error(`Markdown contains HTML: ${context}`);
  }

  text = text.replace(/&[a-z]+;|&#\d+;/g, entity => {
    if (!HTML_ENTITIES[entity]) throw new Error(`Unknown HTML entity ${entity} in: ${context}`);
    return HTML_ENTITIES[entity];
  });

  let result = '';
  let lastIndex = 0;
  for (const m of text.matchAll(EMPHASIS_REGEXP)) {
    result += escapeDocMarkdown(text.slice(lastIndex, m.index));
    result += `*${escapeDocMarkdown(m[2])}*`;
    lastIndex = m.index + m[0].length;
  }
  return result + escapeDocMarkdown(text.slice(lastIndex));
}

function sanitizeLine(line, context) {
  // Keep `code` spans as they are and sanitize the text around them
  return line
    .split(/(`[^`]*`)/)
    .map((part, i) => (i % 2 ? part : sanitizeText(part, context)))
    .join('');
}

/**
 * Converts the markdown from the docs to "doc markdown", the simplified markdown that the console renders (see
 * renderDocMarkdown in src/components/code-editor/doc-markdown.ts):
 * - `code` spans
 * - *emphasis*
 * - line breaks (\n)
 * - list items (lines that start with "- ")
 * - a backslash escapes the next character
 * Links are reduced to their text and HTML is not allowed (other than <br> and simple lists, which are converted).
 */
function removeTrailingBreaks(text) {
  let m;
  while ((m = /<br\s*\/?>$/.exec(text))) text = text.slice(0, m.index);
  return text;
}

function sanitizeMarkdown(markdown) {
  const context = markdown;

  const lines = removeTrailingBreaks(
    markdown
      .replace(/\[([^[\]]*)\]\([^)]*\)/g, '$1') // Remove links
      .split(/<ul>(.*?)<\/ul>/) // The odd parts are the list items
      .map((part, i, parts) => {
        if (i % 2) return part.replace(/<li>(.*?)<\/li>/g, '<br>\u0000$1') + '<br>';
        return i < parts.length - 1 ? removeTrailingBreaks(part) : part; // The breaks before a list are dropped
      })
      .join(''),
  ).split(/<br\s*\/?>/);

  return lines
    .map(line => {
      if (line.startsWith('\u0000')) return `- ${sanitizeLine(line.slice(1), context)}`;
      line = sanitizeLine(line, context);
      return line.startsWith('- ') ? `\\${line}` : line; // Not a list item
    })
    .join('\n')
    .trim();
}

const readDoc = async () => {
  // Read the list of files from sql-doc-files.txt
  const fileList = await fs.readFile(INPUT_FILELIST_FILE, 'utf-8');
  const filePaths = fileList
    .split('\n')
    .map(line => line.trim())
    .filter(line => line && !line.startsWith('#')); // Skip empty lines and comments

  // Read all files in parallel
  const fileContents = await Promise.all(filePaths.map(filePath => fs.readFile(filePath, 'utf-8')));

  const data = fileContents.join('\n');

  const lines = data.split('\n');

  const functionDocs = initialFunctionDocs;
  const dataTypeDocs = {};
  for (const line of lines) {
    const functionMatch = line.match(/^\|\s*`(\w+)\(([^|]*)\)`\s*\|([^|]+)\|(?:([^|]+)\|)?$/);
    if (functionMatch) {
      const functionName = functionMatch[1];
      const args = sanitizeArguments(functionMatch[2]);
      const description = sanitizeMarkdown(functionMatch[3].trim());
      functionDocs[functionName] = [args, description];
    }

    const dataTypeMatch = line.match(/^\|([A-Z]+)\|([A-Z]+)\|([^|]*)\|([^|]*)\|$/);
    if (dataTypeMatch) {
      dataTypeDocs[dataTypeMatch[1]] = [dataTypeMatch[2], sanitizeMarkdown(dataTypeMatch[4])];
    }
  }

  // Make sure there are enough functions found
  const numFunction = Object.keys(functionDocs).length;
  if (!(MINIMUM_EXPECTED_NUMBER_OF_FUNCTIONS <= numFunction)) {
    throw new Error(
      `Did not find enough function entries did the file structure change? (found ${numFunction} but expected at least ${MINIMUM_EXPECTED_NUMBER_OF_FUNCTIONS})`,
    );
  }

  // Make sure there are at least 10 data types for sanity
  const numDataTypes = Object.keys(dataTypeDocs).length;
  if (!(MINIMUM_EXPECTED_NUMBER_OF_DATA_TYPES <= numDataTypes)) {
    throw new Error(
      `Did not find enough data type entries did the file structure change? (found ${numDataTypes} but expected at least ${MINIMUM_EXPECTED_NUMBER_OF_DATA_TYPES})`,
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

// The descriptions are in "doc markdown", see sanitizeMarkdown in script/create-sql-docs.mjs

// prettier-ignore
export const SQL_DATA_TYPES = new Map<string, [runtime: string, description: string]>(Object.entries(${JSON.stringify(
    dataTypeDocs,
    null,
    2,
  )}));

// prettier-ignore
export const SQL_FUNCTIONS = new Map<string, [args: string, description: string]>(Object.entries(${JSON.stringify(
    functionDocs,
    null,
    2,
  )}));
`;

  // eslint-disable-next-line no-undef
  console.log(`Found ${numDataTypes} data types and ${numFunction} functions`);
  await fs.writeFile(OUTPUT_FILE, content, 'utf-8');
};

readDoc();
