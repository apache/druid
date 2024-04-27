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

import type { Column, LiteralValue, QueryResult, SqlExpression } from '@druid-toolkit/query';
import {
  C,
  F,
  L,
  RefName,
  SqlAlias,
  SqlColumnList,
  SqlQuery,
  SqlRecord,
  SqlValues,
} from '@druid-toolkit/query';

import { oneOf } from './general';

const SAMPLE_ARRAY_SEPARATOR = '<#>'; // Note that this is a regexp so don't add anything that is a special regexp thing

function nullForColumn(column: Column): LiteralValue {
  return oneOf(column.sqlType, 'BIGINT', 'DOUBLE', 'FLOAT') ? 0 : '';
}

export function sampleDataToQuery(sample: QueryResult): SqlQuery {
  const { header, rows } = sample;
  return SqlQuery.create(
    new SqlAlias({
      expression: SqlValues.create(
        rows.map(row =>
          SqlRecord.create(
            row.map((r, i) => {
              if (header[i].nativeType === 'COMPLEX<json>') {
                return L(JSON.stringify(r));
              } else if (String(header[i].sqlType).endsWith(' ARRAY')) {
                return L(r.join(SAMPLE_ARRAY_SEPARATOR));
              } else if (r == null || typeof r === 'object') {
                // Avoid actually using NULL literals as they create havoc in the VALUES type system and throw errors.
                // Also, cleanup array if it happens to get here, it shouldn't.
                return L(nullForColumn(header[i]));
              } else {
                return L(r);
              }
            }),
          ),
        ),
      ),
      alias: RefName.alias('t'),
      columns: SqlColumnList.create(header.map((_, i) => RefName.create(`c${i}`, true))),
    }),
  ).changeSelectExpressions(
    header.map(({ name, nativeType, sqlType }, i) => {
      let ex: SqlExpression = C(`c${i}`);
      if (nativeType === 'COMPLEX<json>') {
        ex = F('PARSE_JSON', ex);
      } else if (sqlType && sqlType.endsWith(' ARRAY')) {
        ex = F('STRING_TO_ARRAY', ex, SAMPLE_ARRAY_SEPARATOR);
        if (sqlType !== 'VARCHAR ARRAY') {
          ex = ex.cast(sqlType);
        }
      } else if (sqlType) {
        ex = ex.cast(sqlType);
      }
      return ex.as(name, true);
    }),
  );
}
