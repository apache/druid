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
  SqlType,
  SqlValues,
} from '@druid-toolkit/query';

import { oneOf } from './general';

const SAMPLE_ARRAY_SEPARATOR = '<#>'; // Note that this is a regexp so don't add anything that is a special regexp thing

function getEffectiveSqlType(column: Column): string | undefined {
  const sqlType = column.sqlType;
  if (sqlType === 'ARRAY' && String(column.nativeType).startsWith('ARRAY<')) {
    return `${SqlType.fromNativeType(String(column.nativeType).slice(6, -1))} ARRAY`;
  }
  return sqlType;
}

function nullForSqlType(sqlType: string | undefined): LiteralValue {
  return oneOf(sqlType, 'BIGINT', 'DOUBLE', 'FLOAT') ? 0 : '';
}

export function queryResultToValuesQuery(sample: QueryResult): SqlQuery {
  const { header, rows } = sample;
  return SqlQuery.create(
    new SqlAlias({
      expression: SqlValues.create(
        rows.map(row =>
          SqlRecord.create(
            row.map((r, i) => {
              const column = header[i];
              const { nativeType } = column;
              const sqlType = getEffectiveSqlType(column);
              if (nativeType === 'COMPLEX<json>') {
                return L(JSON.stringify(r));
              } else if (String(sqlType).endsWith(' ARRAY')) {
                return L(r.join(SAMPLE_ARRAY_SEPARATOR));
              } else if (
                sqlType === 'OTHER' &&
                String(nativeType).startsWith('COMPLEX<') &&
                typeof r === 'string' &&
                r.startsWith('"') &&
                r.endsWith('"')
              ) {
                // r is a JSON encoded base64 string
                return L(r.slice(1, -1));
              } else if (r == null || typeof r === 'object') {
                // Avoid actually using NULL literals as they create havoc in the VALUES type system and throw errors.
                // Also, cleanup array if it happens to get here, it shouldn't.
                return L(nullForSqlType(sqlType));
              } else {
                return L(r);
              }
            }),
          ),
        ),
      ),
      alias: RefName.alias('t'),
      columns: SqlColumnList.create(header.map((_, i) => RefName.create(`c${i + 1}`, true))),
    }),
  ).changeSelectExpressions(
    header.map((column, i) => {
      const { name, nativeType } = column;
      const sqlType = getEffectiveSqlType(column);
      let ex: SqlExpression = C(`c${i + 1}`);
      if (nativeType === 'COMPLEX<json>') {
        ex = F('PARSE_JSON', ex);
      } else if (String(sqlType).endsWith(' ARRAY')) {
        ex = F('STRING_TO_ARRAY', ex, SAMPLE_ARRAY_SEPARATOR);
        if (sqlType && sqlType !== 'ARRAY' && sqlType !== 'VARCHAR ARRAY') {
          ex = ex.cast(sqlType);
        }
      } else if (sqlType === 'OTHER' && String(nativeType).startsWith('COMPLEX<')) {
        ex = F('DECODE_BASE64_COMPLEX', String(nativeType).slice(8, -1), ex);
      } else if (sqlType && sqlType !== 'OTHER') {
        ex = ex.cast(sqlType);
      }
      return ex.as(name, true);
    }),
  );
}
