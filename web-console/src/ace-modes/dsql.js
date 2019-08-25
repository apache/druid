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

var druidKeywords = require('../../lib/keywords');
var druidFunctions = require('../../lib/sql-docs');

ace.define(
  'ace/mode/dsql_highlight_rules',
  ['require', 'exports', 'module', 'ace/lib/oop', 'ace/mode/text_highlight_rules'],
  function(acequire, exports, module) {
    'use strict';

    var oop = acequire('../lib/oop');
    var TextHighlightRules = acequire('./text_highlight_rules').TextHighlightRules;

    var SqlHighlightRules = function() {
      // Stuff like: 'with|select|from|where|and|or|group|by|order|limit|having|as|case|'
      var keywords = druidKeywords.SQL_KEYWORDS.concat(druidKeywords.SQL_EXPRESSION_PARTS)
        .join('|')
        .replace(/\s/g, '|');

      // Stuff like: 'true|false'
      var builtinConstants = druidKeywords.SQL_CONSTANTS.join('|');

      // Stuff like: 'avg|count|first|last|max|min'
      var builtinFunctions = druidKeywords.SQL_DYNAMICS.concat(
        druidFunctions.SQL_FUNCTIONS.map(function(f) {
          return f.name;
        }),
      ).join('|');

      // Stuff like: 'int|numeric|decimal|date|varchar|char|bigint|float|double|bit|binary|text|set|timestamp'
      var dataTypes = druidFunctions.SQL_DATA_TYPES.map(function(f) {
        return f.name;
      }).join('|');

      var keywordMapper = this.createKeywordMapper(
        {
          'support.function': builtinFunctions,
          keyword: keywords,
          'constant.language': builtinConstants,
          'storage.type': dataTypes,
        },
        'identifier',
        true,
      );

      this.$rules = {
        start: [
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
            token: 'string', // " string
            regex: '".*?"',
          },
          {
            token: 'string', // ' string
            regex: "'.*?'",
          },
          {
            token: 'string', // ` string (apache drill)
            regex: '`.*?`',
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
  function(acequire, exports, module) {
    'use strict';

    var oop = acequire('../lib/oop');
    var TextMode = acequire('./text').Mode;
    var SqlHighlightRules = acequire('./dsql_highlight_rules').SqlHighlightRules;

    var Mode = function() {
      this.HighlightRules = SqlHighlightRules;
      this.$behaviour = this.$defaultBehaviour;
    };
    oop.inherits(Mode, TextMode);

    (function() {
      this.lineCommentStart = '--';

      this.$id = 'ace/mode/dsql';
    }.call(Mode.prototype));

    exports.Mode = Mode;
  },
);
