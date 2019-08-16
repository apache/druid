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
// https://github.com/thlorenz/brace/blob/master/mode/hjson.js
// Originally licensed under the MIT license (https://github.com/thlorenz/brace/blob/master/LICENSE)
// This file was modified to remove the folding functionality that did not play nice when loaded along side the
// sql mode (which does not have any folding function)

ace.define(
  'ace/mode/hjson_highlight_rules',
  ['require', 'exports', 'module', 'ace/lib/oop', 'ace/mode/text_highlight_rules'],
  function(acequire, exports, module) {
    'use strict';

    var oop = acequire('../lib/oop');
    var TextHighlightRules = acequire('./text_highlight_rules').TextHighlightRules;

    var HjsonHighlightRules = function() {
      this.$rules = {
        start: [
          {
            include: '#comments',
          },
          {
            include: '#rootObject',
          },
          {
            include: '#value',
          },
        ],
        '#array': [
          {
            token: 'paren.lparen',
            regex: /\[/,
            push: [
              {
                token: 'paren.rparen',
                regex: /\]/,
                next: 'pop',
              },
              {
                include: '#value',
              },
              {
                include: '#comments',
              },
              {
                token: 'text',
                regex: /,|$/,
              },
              {
                token: 'invalid.illegal',
                regex: /[^\s\]]/,
              },
              {
                defaultToken: 'array',
              },
            ],
          },
        ],
        '#comments': [
          {
            token: ['comment.punctuation', 'comment.line'],
            regex: /(#)(.*$)/,
          },
          {
            token: 'comment.punctuation',
            regex: /\/\*/,
            push: [
              {
                token: 'comment.punctuation',
                regex: /\*\//,
                next: 'pop',
              },
              {
                defaultToken: 'comment.block',
              },
            ],
          },
          {
            token: ['comment.punctuation', 'comment.line'],
            regex: /(\/\/)(.*$)/,
          },
        ],
        '#constant': [
          {
            token: 'constant',
            regex: /\b(?:true|false|null)\b/,
          },
        ],
        '#keyname': [
          {
            token: 'keyword',
            regex: /(?:[^,\{\[\}\]\s]+|"(?:[^"\\]|\\.)*")\s*(?=:)/,
          },
        ],
        '#mstring': [
          {
            token: 'string',
            regex: /'''/,
            push: [
              {
                token: 'string',
                regex: /'''/,
                next: 'pop',
              },
              {
                defaultToken: 'string',
              },
            ],
          },
        ],
        '#number': [
          {
            token: 'constant.numeric',
            regex: /-?(?:0|[1-9]\d*)(?:(?:\.\d+)?(?:[eE][+-]?\d+)?)?/,
            comment: 'handles integer and decimal numbers',
          },
        ],
        '#object': [
          {
            token: 'paren.lparen',
            regex: /\{/,
            push: [
              {
                token: 'paren.rparen',
                regex: /\}/,
                next: 'pop',
              },
              {
                include: '#keyname',
              },
              {
                include: '#value',
              },
              {
                token: 'text',
                regex: /:/,
              },
              {
                token: 'text',
                regex: /,/,
              },
              {
                defaultToken: 'paren',
              },
            ],
          },
        ],
        '#rootObject': [
          {
            token: 'paren',
            regex: /(?=\s*(?:[^,\{\[\}\]\s]+|"(?:[^"\\]|\\.)*")\s*:)/,
            push: [
              {
                token: 'paren.rparen',
                regex: /---none---/,
                next: 'pop',
              },
              {
                include: '#keyname',
              },
              {
                include: '#value',
              },
              {
                token: 'text',
                regex: /:/,
              },
              {
                token: 'text',
                regex: /,/,
              },
              {
                defaultToken: 'paren',
              },
            ],
          },
        ],
        '#string': [
          {
            token: 'string',
            regex: /"/,
            push: [
              {
                token: 'string',
                regex: /"/,
                next: 'pop',
              },
              {
                token: 'constant.language.escape',
                regex: /\\(?:["\\\/bfnrt]|u[0-9a-fA-F]{4})/,
              },
              {
                token: 'invalid.illegal',
                regex: /\\./,
              },
              {
                defaultToken: 'string',
              },
            ],
          },
        ],
        '#ustring': [
          {
            token: 'string',
            regex: /\b[^:,0-9\-\{\[\}\]\s].*$/,
          },
        ],
        '#value': [
          {
            include: '#constant',
          },
          {
            include: '#number',
          },
          {
            include: '#string',
          },
          {
            include: '#array',
          },
          {
            include: '#object',
          },
          {
            include: '#comments',
          },
          {
            include: '#mstring',
          },
          {
            include: '#ustring',
          },
        ],
      };

      this.normalizeRules();
    };

    HjsonHighlightRules.metaData = {
      fileTypes: ['hjson'],
      keyEquivalent: '^~J',
      name: 'Hjson',
      scopeName: 'source.hjson',
    };

    oop.inherits(HjsonHighlightRules, TextHighlightRules);

    exports.HjsonHighlightRules = HjsonHighlightRules;
  },
);

ace.define(
  'ace/mode/hjson',
  [
    'require',
    'exports',
    'module',
    'ace/lib/oop',
    'ace/mode/text',
    'ace/mode/hjson_highlight_rules',
  ],
  function(acequire, exports, module) {
    'use strict';

    var oop = acequire('../lib/oop');
    var TextMode = acequire('./text').Mode;
    var HjsonHighlightRules = acequire('./hjson_highlight_rules').HjsonHighlightRules;

    var Mode = function() {
      this.HighlightRules = HjsonHighlightRules;
    };
    oop.inherits(Mode, TextMode);

    (function() {
      this.lineCommentStart = '//';
      this.blockComment = { start: '/*', end: '*/' };
      this.$id = 'ace/mode/hjson';
    }.call(Mode.prototype));

    exports.Mode = Mode;
  },
);
