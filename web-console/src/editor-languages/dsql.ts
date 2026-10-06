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

import { closeBrackets, closeBracketsKeymap } from '@codemirror/autocomplete';
import type { Language } from '@codemirror/language';
import { LanguageSupport, StreamLanguage } from '@codemirror/language';
import { keymap } from '@codemirror/view';
import { dedupe } from 'druid-query-toolkit';

import { SQL_CONSTANTS, SQL_DYNAMICS, SQL_KEYWORDS } from '../../lib/keywords';
import { SQL_DATA_TYPES, SQL_FUNCTIONS } from '../../lib/sql-docs';
import { makeCompletionSource } from '../components/code-editor/completion-source';
import type { GetSqlCompletionsOptions } from '../editor-completions/sql-completions';
import { getSqlCompletions } from '../editor-completions/sql-completions';
import type { AvailableFunctions } from '../helpers';

import { createRuleParser, TOKEN_TABLE } from './rule-parser';

function makeWordTokens(
  availableSqlFunctions: AvailableFunctions | undefined,
): Map<string, string> {
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

  const wordTokens = new Map<string, string>();
  for (const [token, words] of tokenLists) {
    for (const word of words) wordTokens.set(word.toLowerCase(), token);
  }
  return wordTokens;
}

function makeDsqlLanguage(availableSqlFunctions: AvailableFunctions | undefined): Language {
  let wordTokens: Map<string, string> | undefined;
  const getWordTokens = () => (wordTokens ??= makeWordTokens(availableSqlFunctions));

  return StreamLanguage.define({
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
}

const defaultDsqlLanguage = makeDsqlLanguage(undefined);
const dsqlLanguages = new WeakMap<AvailableFunctions, Language>();

/**
 * The DruidSQL language. The functions that the cluster has (in addition to the documented ones) are highlighted as
 * functions. The same functions always give the same language so that reconfiguring an editor does not re-parse it.
 */
export function getDsqlLanguage(availableSqlFunctions?: AvailableFunctions): Language {
  if (!availableSqlFunctions) return defaultDsqlLanguage;
  let language = dsqlLanguages.get(availableSqlFunctions);
  if (!language) {
    language = makeDsqlLanguage(availableSqlFunctions);
    dsqlLanguages.set(availableSqlFunctions, language);
  }
  return language;
}

export type DsqlOptions = Pick<
  GetSqlCompletionsOptions,
  'columnMetadata' | 'columns' | 'availableSqlFunctions' | 'skipAggregates'
>;

/**
 * DruidSQL support for a CodeEditor: highlighting, completions and bracket closing
 */
export function dsql(options: DsqlOptions = {}): LanguageSupport {
  const language = getDsqlLanguage(options.availableSqlFunctions);
  return new LanguageSupport(language, [
    language.data.of({
      autocomplete: makeCompletionSource(
        ({ allText, prefix, charBeforePrefix, lineBeforePrefix }) =>
          getSqlCompletions({ ...options, allText, prefix, charBeforePrefix, lineBeforePrefix }),
      ),
    }),
    closeBrackets(),
    keymap.of(closeBracketsKeymap),
  ]);
}
