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

import type { SqlBase } from '@druid-toolkit/query';
import {
  SqlColumn,
  SqlExpression,
  SqlFunction,
  SqlLiteral,
  SqlQuery,
  SqlStar,
} from '@druid-toolkit/query';

import type { RowColumn } from './general';
import { offsetToRowColumn } from './general';

export function prettyPrintSql(b: SqlBase): string {
  return b.prettyTrim(50).toString();
}

export function timeFormatToSql(timeFormat: string): SqlExpression | undefined {
  switch (timeFormat) {
    case 'auto':
      return SqlExpression.parse('TIME_PARSE(TRIM(?))');

    case 'iso':
      return SqlExpression.parse('TIME_PARSE(?)');

    case 'posix':
      return SqlExpression.parse(`MILLIS_TO_TIMESTAMP((?) * 1000)`);

    case 'millis':
      return SqlExpression.parse(`MILLIS_TO_TIMESTAMP(?)`);

    case 'micro':
      return SqlExpression.parse(`MILLIS_TO_TIMESTAMP((?) / 1000)`);

    case 'nano':
      return SqlExpression.parse(`MILLIS_TO_TIMESTAMP((?) / 1000000)`);

    default:
      return SqlExpression.parse(`TIME_PARSE(?, '${timeFormat}')`);
  }
}

export function convertToGroupByExpression(ex: SqlExpression): SqlExpression | undefined {
  const underlyingExpression = ex.getUnderlyingExpression();
  if (!(underlyingExpression instanceof SqlFunction)) return;

  const args = underlyingExpression.args?.values;
  if (!args) return;

  const interestingArgs = args.filter(
    arg => !(arg instanceof SqlLiteral || arg instanceof SqlStar),
  );
  if (interestingArgs.length !== 1) return;

  const newEx = interestingArgs[0];
  if (newEx instanceof SqlColumn) return newEx;

  return newEx.as((ex.getOutputName() || 'grouped').replace(/^[a-z]+_/i, ''));
}

function extractQueryPrefix(text: string): string {
  let q = SqlQuery.parse(text);

  // The parser will parse a SELECT query with a partitionedByClause and clusteredByClause but that is not valid, remove them from the query
  if (!q.getIngestTable() && (q.partitionedByClause || q.clusteredByClause)) {
    q = q.changePartitionedByClause(undefined).changeClusteredByClause(undefined);
  }

  return q.toString().trimEnd();
}

export function findSqlQueryPrefix(text: string): string | undefined {
  try {
    return extractQueryPrefix(text);
  } catch (e) {
    const startOffset = e.location?.start?.offset;
    if (typeof startOffset !== 'number') return;
    const prefix = text.slice(0, startOffset);
    // Try to trim to where the error came from
    try {
      return extractQueryPrefix(prefix);
    } catch {
      // Try to trim out last word
      try {
        return extractQueryPrefix(prefix.replace(/\s*\w+$/, ''));
      } catch {
        return;
      }
    }
  }
}

export function cleanSqlQueryPrefix(text: string): string {
  const matchReplace = text.match(/\sREPLACE$/i);
  if (matchReplace) {
    // This query likely grabbed a "REPLACE" (which is not a reserved keyword) from the next query over, see if we can delete it
    const textWithoutReplace = text.slice(0, -matchReplace[0].length).trimEnd();
    if (SqlQuery.maybeParse(textWithoutReplace)) {
      return textWithoutReplace;
    }
  }

  return text;
}

export interface QuerySlice {
  index: number;
  startOffset: number;
  startRowColumn: RowColumn;
  endOffset: number;
  endRowColumn: RowColumn;
  sql: string;
}

export function findAllSqlQueriesInText(text: string): QuerySlice[] {
  const found: QuerySlice[] = [];

  let remainingText = text;
  let offset = 0;
  let m: RegExpExecArray | null = null;
  do {
    m = /SELECT|WITH|INSERT|REPLACE|EXPLAIN/i.exec(remainingText);
    if (m) {
      const sql = findSqlQueryPrefix(remainingText.slice(m.index));
      const advanceBy = m.index + m[0].length; // Skip the initial word
      if (sql) {
        const endIndex = m.index + sql.length;
        found.push({
          index: found.length,
          startOffset: offset + m.index,
          startRowColumn: offsetToRowColumn(text, offset + m.index)!,
          endOffset: offset + endIndex,
          endRowColumn: offsetToRowColumn(text, offset + endIndex)!,
          sql: cleanSqlQueryPrefix(sql),
        });
      }
      remainingText = remainingText.slice(advanceBy);
      offset += advanceBy;
    }
  } while (m);

  return found;
}
