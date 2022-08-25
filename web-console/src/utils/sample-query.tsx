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

import {
  Column,
  LiteralValue,
  QueryResult,
  RefName,
  SqlAlias,
  SqlColumnList,
  SqlExpression,
  SqlFunction,
  SqlLiteral,
  SqlQuery,
  SqlRecord,
  SqlRef,
  SqlValues,
} from 'druid-query-toolkit';

import { oneOf } from './general';

const SAMPLE_ARRAY_SEPARATOR = '-3432-d401-';

function nullForColumn(column: Column): LiteralValue {
  return oneOf(column.sqlType, 'BIGINT', 'DOUBLE', 'FLOAT') ? 0 : '';
}

export function sampleDataToQuery(sample: QueryResult): SqlQuery {
  const { header, rows } = sample;
  const arrayIndexes: Record<number, boolean> = {};
  return SqlQuery.create(
    new SqlAlias({
      expression: SqlValues.create(
        rows.map(row =>
          SqlRecord.create(
            row.map((r, i) => {
              if (header[i].nativeType === 'COMPLEX<json>') {
                return SqlLiteral.create(JSON.stringify(r));
              } else if (Array.isArray(r)) {
                arrayIndexes[i] = true;
                return SqlLiteral.create(r.join(SAMPLE_ARRAY_SEPARATOR));
              } else {
                // Avoid actually using NULL literals as they create havc in the VALUES type system and throw errors.
                return SqlLiteral.create(r == null ? nullForColumn(header[i]) : r);
              }
            }),
          ),
        ),
      ),
      alias: RefName.alias('t'),
      columns: SqlColumnList.create(header.map((_, i) => RefName.create(`c${i}`, true))),
    }),
  ).changeSelectExpressions(
    header.map((h, i) => {
      let ex: SqlExpression = SqlRef.column(`c${i}`);
      if (h.nativeType === 'COMPLEX<json>') {
        ex = SqlFunction.simple('PARSE_JSON', [ex]);
      } else if (arrayIndexes[i]) {
        ex = SqlFunction.simple('STRING_TO_MV', [ex, SqlLiteral.create(SAMPLE_ARRAY_SEPARATOR)]);
      } else if (h.sqlType) {
        ex = ex.cast(h.sqlType);
      }
      return ex.as(h.name, true);
    }),
  );
}
