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

// The highlighting rules are a port of the Ace mode located at
// https://github.com/thlorenz/brace/blob/master/mode/sql.js
// Originally licensed under the MIT license (https://github.com/thlorenz/brace/blob/master/LICENSE)
// The list of keywords was modified to more closely adhere to what is found in DruidSQL

import { StreamLanguage } from '@codemirror/language';
import { dedupe } from 'druid-query-toolkit';

import { SQL_CONSTANTS, SQL_DYNAMICS, SQL_KEYWORDS } from '../../lib/keywords';
import { SQL_DATA_TYPES, SQL_FUNCTIONS } from '../../lib/sql-docs';
import type { AvailableFunctions } from '../helpers';

import { createRuleParser, TOKEN_TABLE } from './rule-parser';

let wordTokens: Map<string, string> | undefined;
let availableSqlFunctions: AvailableFunctions | undefined;

function getWordTokens(): Map<string, string> {
  if (wordTokens) return wordTokens;

  // A word that is in several lists gets the token of the last one
  const tokenLists: [string, string[]][] = [
    [
      'function',
      dedupe([
        ...SQL_DYNAMICS,
        ...Array.from(SQL_FUNCTIONS.keys()),
        ...(availableSqlFunctions?.keys() || []),
      ]),
    ],
    ['keyword', SQL_KEYWORDS.flatMap(k => k.split(/\s/g))], // Some keywords are like "EXPLAIN PLAN FOR"
    ['constant', SQL_CONSTANTS],
    ['typeName', Array.from(SQL_DATA_TYPES.keys())],
  ];

  wordTokens = new Map();
  for (const [token, words] of tokenLists) {
    for (const word of words) wordTokens.set(word.toLowerCase(), token);
  }
  return wordTokens;
}

export const dsqlLanguage = StreamLanguage.define({
  name: 'dsql',
  ...createRuleParser({
    start: [
      { token: 'issue', regex: /--:ISSUE:.*$/ },
      { token: 'comment', regex: /--.*$/ },
      { token: 'comment', regex: /\/\*/, push: 'blockComment' },
      { token: 'column', regex: /".*?"/ }, // " quoted reference
      { token: 'string', regex: /'.*?'/ }, // ' string literal
      { token: 'number', regex: /[+-]?\d+(?:(?:\.\d*)?(?:[eE][+-]?\d+)?)?\b/ },
      {
        token: word => getWordTokens().get(word.toLowerCase()) ?? null,
        regex: /[a-zA-Z_$][a-zA-Z0-9_$]*\b/,
      },
      { token: 'operator', regex: /\+|-|\/|\/\/|%|<@>|@>|<@|&|\^|~|<|>|<=|=>|==|!=|<>|=/ },
      { token: 'paren', regex: /[()]/ },
    ],
    blockComment: [
      { token: 'comment', regex: /\*\//, pop: true },
      { token: 'comment', regex: /(?:[^*]|\*(?!\/))+/ },
    ],
  }),
  tokenTable: TOKEN_TABLE,
  languageData: {
    commentTokens: { line: '--' },
    // Like Ace, do not auto close braces (they are rare in SQL and typing one usually means the start of a JSON query)
    closeBrackets: { brackets: ['(', '[', "'", '"'] },
  },
});

/**
 * Highlights the functions that the cluster has (in addition to the documented ones) from now on
 */
export function initDsqlMode(sqlFunctions: AvailableFunctions | undefined) {
  availableSqlFunctions = sqlFunctions;
  wordTokens = undefined;
}
