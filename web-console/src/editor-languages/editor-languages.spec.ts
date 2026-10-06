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
    expect(tokenOf(dsqlLanguage, sql, '--:ISSUE: bad')).toEqual('issue');
    expect(tokenOf(dsqlLanguage, sql, 'SELECT')).toEqual('keyword');
    expect(tokenOf(dsqlLanguage, sql, 'COUNT')).toEqual('function');
    expect(tokenOf(dsqlLanguage, sql, '"col"')).toEqual('column');
    expect(tokenOf(dsqlLanguage, sql, 'VARCHAR')).toEqual('typeName');
    expect(tokenOf(dsqlLanguage, sql, 'TRUE')).toEqual('constant');
    expect(tokenOf(dsqlLanguage, sql, "'lit'")).toEqual('string');
    expect(tokenOf(dsqlLanguage, sql, '3.5')).toEqual('number');
    expect(tokenOf(dsqlLanguage, sql, '<>')).toEqual('operator');
    expect(tokenOf(dsqlLanguage, sql, '-- comment')).toEqual('comment');
    expect(tokenOf(dsqlLanguage, sql, 'line */')).toEqual('comment');
    expect(tokenOf(dsqlLanguage, sql, 'x')).toBeUndefined();
  });

  it('highlights the available functions', () => {
    const availableSqlFunctions = new Map([['MY_FN', { args: ['x'], isAggregate: false }]]);
    expect(tokenOf(dsqlLanguage, 'SELECT MY_FN(x)', 'MY_FN')).toBeUndefined();
    expect(tokenOf(getDsqlLanguage(availableSqlFunctions), 'SELECT MY_FN(x)', 'MY_FN')).toEqual(
      'function',
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
}`;
    expect(tokenOf(hjsonLanguage, hjson, '# comment')).toEqual('comment');
    expect(tokenOf(hjsonLanguage, hjson, 'key')).toEqual('keyword');
    expect(tokenOf(hjsonLanguage, hjson, '"value')).toEqual('string');
    expect(tokenOf(hjsonLanguage, hjson, '\\n')).toEqual('escape');
    expect(tokenOf(hjsonLanguage, hjson, '-1.5e3')).toEqual('number');
    expect(tokenOf(hjsonLanguage, hjson, 'true')).toEqual('literal');
    expect(tokenOf(hjsonLanguage, hjson, '"two"')).toEqual('string');
    expect(tokenOf(hjsonLanguage, hjson, 'some text')).toEqual('string');
  });

  it('highlights Hjson without the root braces', () => {
    const hjson = `queryType: scan
dataSource: wikipedia`;
    expect(tokenOf(hjsonLanguage, hjson, 'queryType')).toEqual('keyword');
    expect(tokenOf(hjsonLanguage, hjson, 'dataSource')).toEqual('keyword');
    expect(tokenOf(hjsonLanguage, hjson, 'wikipedia')).toEqual('string');
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
