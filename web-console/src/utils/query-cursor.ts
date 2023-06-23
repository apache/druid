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

import type { SqlBase, SqlQuery } from 'druid-query-toolkit';
import { L } from 'druid-query-toolkit';

export const EMPTY_LITERAL = L('');

const CRAZY_STRING = '$.X.@.X.$';
const DOT_DOT_DOT_LITERAL = L('...');

export function prettyPrintSql(b: SqlBase): string {
  return b
    .walk(b => {
      if (b === EMPTY_LITERAL) {
        return DOT_DOT_DOT_LITERAL;
      }
      return b;
    })
    .prettyTrim(50)
    .toString();
}

export interface RowColumn {
  match: string;
  row: number;
  column: number;
  endRow?: number;
  endColumn?: number;
}

export function findEmptyLiteralPosition(query: SqlQuery): RowColumn | undefined {
  const subQueryString = query.walk(b => (b === EMPTY_LITERAL ? L(CRAZY_STRING) : b)).toString();

  const crazyIndex = subQueryString.indexOf(CRAZY_STRING);
  if (crazyIndex < 0) return;

  const prefix = subQueryString.slice(0, crazyIndex);
  const lines = prefix.split(/\n/g);
  const row = lines.length - 1;
  const lastLine = lines[row];
  return {
    match: '',
    row,
    column: lastLine.length,
  };
}
