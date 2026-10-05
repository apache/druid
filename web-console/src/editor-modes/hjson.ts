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
// https://github.com/thlorenz/brace/blob/master/mode/hjson.js
// Originally licensed under the MIT license (https://github.com/thlorenz/brace/blob/master/LICENSE)

import { StreamLanguage } from '@codemirror/language';

import type { TokenRule } from './rule-parser';
import { createRuleParser, TOKEN_TABLE } from './rule-parser';

const COMMENTS: TokenRule[] = [
  { token: 'comment', regex: /#.*$/ },
  { token: 'comment', regex: /\/\*/, push: 'blockComment' },
  { token: 'comment', regex: /\/\/.*$/ },
];

const KEY_NAME: TokenRule[] = [
  { token: 'keyword', regex: /(?:[^,{[}\]\s]+|"(?:[^"\\]|\\.)*")\s*(?=:)/ },
];

const VALUE: TokenRule[] = [
  { token: 'literal', regex: /\b(?:true|false|null)\b/ },
  { token: 'number', regex: /-?(?:0|[1-9]\d*)(?:(?:\.\d+)?(?:[eE][+-]?\d+)?)?/ },
  { token: 'string', regex: /"/, push: 'string' },
  { token: 'paren', regex: /\[/, push: 'array' },
  { token: 'paren', regex: /\{/, push: 'object' },
  ...COMMENTS,
  { token: 'string', regex: /'''/, push: 'multilineString' },
  { token: 'string', regex: /\b[^:,0-9\-{[}\]\s].*$/ }, // Unquoted string
];

const OBJECT_CONTENT: TokenRule[] = [...KEY_NAME, ...VALUE, { token: null, regex: /[:,]/ }];

export const hjsonLanguage = StreamLanguage.define({
  name: 'hjson',
  ...createRuleParser({
    start: [
      ...COMMENTS,
      // An object without the braces, it lasts until the end of the text
      { token: null, regex: /(?=\s*(?:[^,{[}\]\s]+|"(?:[^"\\]|\\.)*")\s*:)/, push: 'rootObject' },
      ...VALUE,
    ],
    rootObject: OBJECT_CONTENT,
    object: [{ token: 'paren', regex: /\}/, pop: true }, ...OBJECT_CONTENT],
    array: [
      { token: 'paren', regex: /\]/, pop: true },
      ...VALUE,
      { token: null, regex: /,/ },
      { token: 'invalid', regex: /[^\s\]]/ },
    ],
    string: [
      { token: 'string', regex: /"/, pop: true },
      { token: 'escape', regex: /\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4})/ },
      { token: 'invalid', regex: /\\./ },
      { token: 'string', regex: /[^"\\]+/ },
    ],
    multilineString: [
      { token: 'string', regex: /'''/, pop: true },
      { token: 'string', regex: /(?:[^']|'(?!''))+/ },
    ],
    blockComment: [
      { token: 'comment', regex: /\*\//, pop: true },
      { token: 'comment', regex: /(?:[^*]|\*(?!\/))+/ },
    ],
  }),
  tokenTable: TOKEN_TABLE,
  languageData: {
    commentTokens: { line: '//', block: { open: '/*', close: '*/' } },
  },
});
