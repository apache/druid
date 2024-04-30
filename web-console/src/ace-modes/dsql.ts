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

// This file a modified version of the file located at
// https://github.com/thlorenz/brace/blob/master/mode/sql.js
// Originally licensed under the MIT license (https://github.com/thlorenz/brace/blob/master/LICENSE)
// This file was modified to make the list of keywords more closely adhere to what is found in DruidSQL

import type { Ace } from 'ace-builds';
import ace from 'ace-builds/src-noconflict/ace';

import * as druidKeywords from '../../lib/keywords';
import * as druidFunctions from '../../lib/sql-docs';

import type { ItemDescription } from './make-doc-html';
import { makeDocHtml } from './make-doc-html';

ace.define(
  'ace/mode/dsql_highlight_rules',
  ['require', 'exports', 'module', 'ace/lib/oop', 'ace/mode/text_highlight_rules'],
  function (acequire: any, exports: any) {
    'use strict';

    const oop = acequire('../lib/oop');
    const TextHighlightRules = acequire('./text_highlight_rules').TextHighlightRules;

    const SqlHighlightRules = function (this: any) {
      // Stuff like: 'with|select|from|where|and|or|group|by|order|limit|having|as|case|'
      const keywords = druidKeywords.SQL_KEYWORDS.concat(druidKeywords.SQL_EXPRESSION_PARTS)
        .join('|')
        .replace(/\s/g, '|');

      // Stuff like: 'true|false'
      const builtinConstants = druidKeywords.SQL_CONSTANTS.join('|');

      // Stuff like: 'avg|count|first|last|max|min'
      const builtinFunctions = druidKeywords.SQL_DYNAMICS.concat(
        Object.keys(druidFunctions.SQL_FUNCTIONS),
      ).join('|');

      // Stuff like: 'int|numeric|decimal|date|varchar|char|bigint|float|double|bit|binary|text|set|timestamp'
      const dataTypes = Object.keys(druidFunctions.SQL_DATA_TYPES).join('|');

      const keywordMapper = this.createKeywordMapper(
        {
          'support.function': builtinFunctions,
          'keyword': keywords,
          'constant.language': builtinConstants,
          'storage.type': dataTypes,
        },
        'identifier',
        true,
      );

      this.$rules = {
        start: [
          {
            token: 'comment.issue',
            regex: '--:ISSUE:.*$',
          },
          {
            token: 'comment',
            regex: '--.*$',
          },
          {
            token: 'comment',
            start: '/\\*',
            end: '\\*/',
          },
          {
            token: 'variable.column', // " quoted reference
            regex: '".*?"',
          },
          {
            token: 'string', // ' string literal
            regex: "'.*?'",
          },
          {
            token: 'constant.numeric', // float
            regex: '[+-]?\\d+(?:(?:\\.\\d*)?(?:[eE][+-]?\\d+)?)?\\b',
          },
          {
            token: keywordMapper,
            regex: '[a-zA-Z_$][a-zA-Z0-9_$]*\\b',
          },
          {
            token: 'keyword.operator',
            regex: '\\+|\\-|\\/|\\/\\/|%|<@>|@>|<@|&|\\^|~|<|>|<=|=>|==|!=|<>|=',
          },
          {
            token: 'paren.lparen',
            regex: '[\\(]',
          },
          {
            token: 'paren.rparen',
            regex: '[\\)]',
          },
          {
            token: 'text',
            regex: '\\s+',
          },
        ],
      };
      this.normalizeRules();
    };

    oop.inherits(SqlHighlightRules, TextHighlightRules);

    exports.SqlHighlightRules = SqlHighlightRules;
  },
);

ace.define(
  'ace/mode/dsql',
  ['require', 'exports', 'module', 'ace/lib/oop', 'ace/mode/text', 'ace/mode/dsql_highlight_rules'],
  function (acequire: any, exports: any) {
    'use strict';

    const oop = acequire('../lib/oop');
    const TextMode = acequire('./text').Mode;
    const SqlHighlightRules = acequire('./dsql_highlight_rules').SqlHighlightRules;

    const completions = ([] as Ace.Completion[]).concat(
      druidKeywords.SQL_KEYWORDS.map(v => ({ name: v, value: v, score: 0, meta: 'keyword' })),
      druidKeywords.SQL_EXPRESSION_PARTS.map(v => ({
        name: v,
        value: v,
        score: 0,
        meta: 'keyword',
      })),
      druidKeywords.SQL_CONSTANTS.map(v => ({ name: v, value: v, score: 0, meta: 'constant' })),
      druidKeywords.SQL_DYNAMICS.map(v => ({ name: v, value: v, score: 0, meta: 'dynamic' })),
      Object.entries(druidFunctions.SQL_DATA_TYPES).map(([name, [runtime, description]]) => {
        const item: ItemDescription = {
          name,
          description,
          syntax: `Druid runtime type: ${runtime}`,
        };
        return {
          name,
          value: name,
          score: 0,
          meta: 'type',
          docHTML: makeDocHtml(item),
          docText: description,
        };
      }),
      Object.entries(druidFunctions.SQL_FUNCTIONS).flatMap(([name, versions]) => {
        return versions.map(([args, description]) => {
          const item = { name, description, syntax: `${name}(${args})` };
          return {
            name,
            value: versions.length > 1 ? `${name}(${args})` : name,
            score: 1100, // Use a high score to appear over the 'local' suggestions that have a score of 1000
            meta: 'function',
            docHTML: makeDocHtml(item),
            docText: description,
            completer: {
              insertMatch: (editor: any, data: any) => {
                editor.completer.insertMatch({ value: data.name });
              },
            },
          } as Ace.Completion;
        });
      }),
    );

    const Mode = function (this: any) {
      this.HighlightRules = SqlHighlightRules;
      this.$behaviour = this.$defaultBehaviour;
      this.$id = 'ace/mode/dsql';

      this.lineCommentStart = '--';
      this.getCompletions = () => completions;
    };
    oop.inherits(Mode, TextMode);

    exports.Mode = Mode;
  },
);
