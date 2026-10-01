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

import ace from 'ace-builds';

import { initAceDsqlMode } from './dsql';

function tokenize(mode: string, text: string): [type: string, value: string][][] {
  const session = ace.createEditSession(text);
  session.setMode(`ace/mode/${mode}`);
  const lines: [string, string][][] = [];
  for (let row = 0; row < session.getLength(); row++) {
    lines.push(session.getTokens(row).map(({ type, value }) => [type, value]));
  }
  return lines;
}

function tokenOf(mode: string, text: string, value: string): string | undefined {
  return tokenize(mode, text)
    .flat()
    .find(t => t[1] === value)?.[0];
}

describe('ace modes', () => {
  it('highlights DruidSQL', () => {
    const sql = `--:ISSUE: bad
SELECT COUNT(*), "col", CAST(x AS VARCHAR), TRUE FROM t WHERE y <> 'lit' AND z = 3.5 -- comment`;
    expect(tokenOf('dsql', sql, '--:ISSUE: bad')).toEqual('comment.issue');
    expect(tokenOf('dsql', sql, 'SELECT')).toEqual('keyword');
    expect(tokenOf('dsql', sql, 'COUNT')).toEqual('support.function');
    expect(tokenOf('dsql', sql, '"col"')).toEqual('variable.column');
    expect(tokenOf('dsql', sql, 'VARCHAR')).toEqual('storage.type');
    expect(tokenOf('dsql', sql, 'TRUE')).toEqual('constant.language');
    expect(tokenOf('dsql', sql, "'lit'")).toEqual('string');
    expect(tokenOf('dsql', sql, '3.5')).toEqual('constant.numeric');
    expect(tokenOf('dsql', sql, '-- comment')).toEqual('comment');
  });

  it('highlights the available functions in editors created later', () => {
    expect(tokenOf('dsql', 'SELECT MY_FN(x)', 'MY_FN')).toEqual('identifier');
    initAceDsqlMode(new Map([['MY_FN', { args: ['x'], isAggregate: false }]]));
    expect(tokenOf('dsql', 'SELECT MY_FN(x)', 'MY_FN')).toEqual('support.function');
    initAceDsqlMode(undefined);
  });

  it('highlights Hjson', () => {
    const hjson = `{
  # comment
  key: "value"
  n: -1.5e3
  flag: true
  unquoted: some text
}`;
    expect(tokenOf('hjson', hjson, ' comment')).toEqual('comment.line');
    expect(tokenOf('hjson', hjson, 'key')).toEqual('keyword');
    expect(tokenOf('hjson', hjson, '-1.5e3')).toEqual('constant.numeric');
    expect(tokenOf('hjson', hjson, 'true')).toEqual('constant');
    expect(tokenOf('hjson', hjson, 'some text')).toEqual('string');
  });
});
