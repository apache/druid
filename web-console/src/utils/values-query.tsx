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

import type { Column, QueryResult, SqlExpression } from 'druid-query-toolkit';
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
} from 'druid-query-toolkit';
import * as JSONBig from 'json-bigint-native';

import { oneOf } from './general';

const SAMPLE_ARRAY_SEPARATOR = '<#>'; // Note that this is a regexp so don't add anything that is a special regexp thing

/**
 This function corrects for the legacy behaviour where Druid sometimes returns array columns as
 { sqlType: 'ARRAY', nativeType: 'ARRAY<STRING>' }
 instead of the more correct description of
 { sqlType: 'VARCHAR ARRAY', nativeType: 'ARRAY<STRING>' }
 use this function to get the effective SQL type of `VARCHAR ARRAY`
 */
function getEffectiveSqlType(column: Column): string | undefined {
  const sqlType = column.sqlType;
  if (sqlType === 'ARRAY' && String(column.nativeType).startsWith('ARRAY<')) {
    return `${SqlType.fromNativeType(String(column.nativeType).slice(6, -1))} ARRAY`;
  }
  return sqlType;
}

function columnIsAllNulls(rows: readonly unknown[][], columnIndex: number): boolean {
  return rows.every(row => row[columnIndex] === null);
}

function isJsonString(x: unknown): boolean {
  return typeof x === 'string' && oneOf(x[0], '"', '{', '[');
}

export function queryResultToValuesQuery(sample: QueryResult): SqlQuery {
  const { header, rows } = sample;
  return SqlQuery.create(
    new SqlAlias({
      expression: SqlValues.create(
        rows.map(row =>
          SqlRecord.create(
            row.map((d, i) => {
              if (d == null) return L.NULL;
              const column = header[i];
              const { nativeType } = column;
              const sqlType = getEffectiveSqlType(column);
              if (nativeType === 'COMPLEX<json>') {
                return L(isJsonString(d) ? d : JSONBig.stringify(d));
              } else if (String(sqlType).endsWith(' ARRAY')) {
                return L(d.join(SAMPLE_ARRAY_SEPARATOR));
              } else if (
                sqlType === 'OTHER' &&
                String(nativeType).startsWith('COMPLEX<') &&
                typeof d === 'string' &&
                d.startsWith('"') &&
                d.endsWith('"')
              ) {
                // d is a JSON encoded base64 string
                return L(d.slice(1, -1));
              } else if (typeof d === 'object') {
                // Cleanup array if it happens to get here, it shouldn't.
                return L.NULL;
              } else {
                return L(d);
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

      // The columnIsAllNulls check is needed due to https://github.com/apache/druid/issues/16456
      // Remove it when the issue above is resolved
      let ex: SqlExpression = columnIsAllNulls(rows, i) ? L.NULL : C(`c${i + 1}`);
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
