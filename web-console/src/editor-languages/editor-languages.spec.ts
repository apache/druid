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

import type { CompletionResult, CompletionSource } from '@codemirror/autocomplete';
import { CompletionContext } from '@codemirror/autocomplete';
import type { Language } from '@codemirror/language';
import type { Extension } from '@codemirror/state';
import { EditorState } from '@codemirror/state';
import { highlightTree, tags } from '@lezer/highlight';

import { codeEditorHighlightStyle } from '../components/code-editor/code-editor-theme';

import { dsql, getDsqlLanguage } from './dsql';
import { hjson, hjsonLanguage } from './hjson';

const dsqlLanguage = getDsqlLanguage();

function tokenize(language: Language, text: string): [type: string, value: string][] {
  const tokens: [string, string][] = [];
  language.parser.parse(text).iterate({
    enter(node) {
      if (node.type.isTop) return;
      tokens.push([node.name, text.slice(node.from, node.to)]);
    },
  });
  return tokens;
}

function tokenOf(language: Language, text: string, value: string): string | undefined {
  return tokenize(language, text).find(t => t[1] === value)?.[0];
}

describe('editor languages', () => {
  it('highlights DruidSQL', () => {
    const sql = `--:ISSUE: bad
SELECT COUNT(*), "col", CAST(x AS VARCHAR), TRUE FROM t WHERE y <> 'lit' AND z = 3.5 -- comment
/* multi
line */ SELECT`;
    expect(tokenOf(dsqlLanguage, sql, '--:ISSUE: bad')).toEqual('Issue');
    expect(tokenOf(dsqlLanguage, sql, 'SELECT')).toEqual('Keyword');
    expect(tokenOf(dsqlLanguage, sql, 'COUNT')).toEqual('FunctionName');
    expect(tokenOf(dsqlLanguage, sql, '"col"')).toEqual('QuotedIdentifier');
    expect(tokenOf(dsqlLanguage, sql, 'VARCHAR')).toEqual('TypeName');
    expect(tokenOf(dsqlLanguage, sql, 'TRUE')).toEqual('Constant');
    expect(tokenOf(dsqlLanguage, sql, "'lit'")).toEqual('String');
    expect(tokenOf(dsqlLanguage, sql, '3.5')).toEqual('Number');
    expect(tokenOf(dsqlLanguage, sql, '<')).toEqual('Operator');
    expect(tokenOf(dsqlLanguage, sql, '-- comment')).toEqual('LineComment');
    expect(tokenOf(dsqlLanguage, sql, '/* multi\nline */')).toEqual('BlockComment');
    expect(tokenOf(dsqlLanguage, sql, 'x')).toEqual('Identifier');
    expect(tokenOf(dsqlLanguage, sql, '(x AS VARCHAR)')).toEqual('Parens');
  });

  it('highlights unterminated DruidSQL tokens', () => {
    expect(tokenOf(dsqlLanguage, 'SELECT /* not closed', '/* not closed')).toEqual('BlockComment');
    // An unterminated quote is just a character
    expect(tokenize(dsqlLanguage, `SELECT 'x`)).toEqual([
      ['Keyword', 'SELECT'],
      ['Punctuation', "'"],
      ['Identifier', 'x'],
    ]);
    // The sign is part of the number
    expect(tokenOf(dsqlLanguage, 'SELECT a-1', '-1')).toEqual('Number');
  });

  it('highlights the available functions', () => {
    const availableSqlFunctions = new Map([['MY_FN', { args: ['x'], isAggregate: false }]]);
    expect(tokenOf(dsqlLanguage, 'SELECT MY_FN(x)', 'MY_FN')).toEqual('Identifier');
    expect(tokenOf(getDsqlLanguage(availableSqlFunctions), 'SELECT MY_FN(x)', 'MY_FN')).toEqual(
      'FunctionName',
    );
    expect(tokenOf(getDsqlLanguage(availableSqlFunctions), 'SELECT my_fn(x)', 'my_fn')).toEqual(
      'FunctionName',
    );
    expect(getDsqlLanguage(availableSqlFunctions)).toBe(getDsqlLanguage(availableSqlFunctions));
  });

  it('highlights Hjson', () => {
    const hjson = `{
  # comment
  key: "value\\n"
  n: -1.5e3
  flag: true
  list: [1, "two"]
  unquoted: some text
  multi: '''
    lines
  '''
}`;
    expect(tokenOf(hjsonLanguage, hjson, '# comment')).toEqual('LineComment');
    expect(tokenOf(hjsonLanguage, hjson, 'key')).toEqual('PropertyName');
    expect(tokenOf(hjsonLanguage, hjson, '"value\\n"')).toEqual('String');
    expect(tokenOf(hjsonLanguage, hjson, '\\n')).toEqual('Escape');
    expect(tokenOf(hjsonLanguage, hjson, '-1.5e3')).toEqual('Number');
    expect(tokenOf(hjsonLanguage, hjson, 'true')).toEqual('True');
    expect(tokenOf(hjsonLanguage, hjson, '[1, "two"]')).toEqual('Array');
    expect(tokenOf(hjsonLanguage, hjson, '"two"')).toEqual('String');
    expect(tokenOf(hjsonLanguage, hjson, 'some text')).toEqual('QuotelessString');
    expect(tokenOf(hjsonLanguage, hjson, "'''\n    lines\n  '''")).toEqual('MultilineString');
  });

  it('highlights JSON', () => {
    const json = `{"auditTime": "2023-07-31T18:15:19.302Z", "url": "http://x:80", "n": 1, "z": null}`;
    expect(tokenOf(hjsonLanguage, json, '"auditTime"')).toEqual('PropertyName');
    expect(tokenOf(hjsonLanguage, json, '"2023-07-31T18:15:19.302Z"')).toEqual('String');
    expect(tokenOf(hjsonLanguage, json, '"http://x:80"')).toEqual('String');
    expect(tokenOf(hjsonLanguage, json, '1')).toEqual('Number');
    expect(tokenOf(hjsonLanguage, json, 'null')).toEqual('Null');
    expect(tokenize(hjsonLanguage, json).some(([type]) => type === '⚠')).toBe(false);
  });

  it('highlights quoteless Hjson values as a whole', () => {
    const hjson = `ip: 127.0.0.1
time: 2023-07-31T18:15:19.302Z
host: localhost:8091
url: http://example.com
count: 3
count2: 3 apples
flag: true, other: 1
page: Talk:Main`;
    for (const value of [
      '127.0.0.1',
      '2023-07-31T18:15:19.302Z',
      'localhost:8091',
      'http://example.com',
      '3 apples',
      'Talk:Main',
    ]) {
      expect(tokenOf(hjsonLanguage, hjson, value)).toEqual('QuotelessString');
    }
    expect(tokenOf(hjsonLanguage, hjson, '3')).toEqual('Number');
    expect(tokenOf(hjsonLanguage, hjson, 'true')).toEqual('True');
  });

  it('highlights Hjson without the root braces', () => {
    const hjson = `queryType: scan
dataSource: wikipedia`;
    expect(tokenOf(hjsonLanguage, hjson, 'queryType')).toEqual('PropertyName');
    expect(tokenOf(hjsonLanguage, hjson, 'dataSource')).toEqual('PropertyName');
    expect(tokenOf(hjsonLanguage, hjson, 'wikipedia')).toEqual('QuotelessString');
    expect(tokenOf(hjsonLanguage, 'just text', 'just text')).toEqual('QuotelessString');
  });

  it('styles Hjson keys like keywords and leaves the literals plain', () => {
    const json = '{"key": null, "flag": true, "s": "x"}';
    const classes = new Map<string, string>();
    highlightTree(hjsonLanguage.parser.parse(json), codeEditorHighlightStyle, (from, to, cls) => {
      classes.set(json.slice(from, to), cls);
    });
    expect(classes.get('"key"')).toEqual(codeEditorHighlightStyle.style([tags.keyword]));
    expect(classes.get('"x"')).toEqual(codeEditorHighlightStyle.style([tags.string]));
    expect(classes.has('null')).toBe(false);
    expect(classes.has('true')).toBe(false);
  });

  it('highlights a key that is being typed', () => {
    expect(tokenize(hjsonLanguage, '{\n  "a": 1\n  que')).toContainEqual(['PropertyName', 'que']);
  });

  describe('completions', () => {
    // Completes at the end of the text (marked with a |) the way the editor would
    function complete(
      extension: Extension,
      textWithCursor: string,
      readOnly = false,
    ): string[] | undefined {
      const pos = textWithCursor.indexOf('|');
      const state = EditorState.create({
        doc: textWithCursor.replace('|', ''),
        extensions: [extension, EditorState.readOnly.of(readOnly)],
      });
      const sources = state.languageDataAt<CompletionSource>('autocomplete', pos);
      if (!sources.length) return;
      const result = sources[0](
        new CompletionContext(state, pos, false),
      ) as CompletionResult | null;
      return result?.options.map(option => option.label);
    }

    it('comes with dsql', () => {
      expect(complete(dsql({ columns: ['my_column'] }), 'SELECT my_c|')).toContain('"my_column"');
      expect(complete(dsql(), 'SELECT COU|')).toContain('COUNT');
    });

    it('comes with hjson when given jsonCompletions', () => {
      const jsonCompletions = [
        { path: '$', isObject: true, completions: [{ value: 'queryType' }] },
      ];
      expect(complete(hjson({ jsonCompletions }), '{\n  que|')).toEqual(['queryType']);
      expect(hjson({ jsonCompletions })).toBe(hjson({ jsonCompletions }));
      expect(complete(hjson(), '{\n  que|')).toBeUndefined();
    });

    it('is off in read only editors', () => {
      expect(complete(dsql(), 'SELECT COU|', true)).toBeUndefined();
    });
  });
});
