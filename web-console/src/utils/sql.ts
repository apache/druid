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

import { SqlExpression, SqlFunction, SqlLiteral, SqlRef, SqlStar } from 'druid-query-toolkit';

export function timeFormatToSql(timeFormat: string): SqlExpression | undefined {
  switch (timeFormat) {
    case 'auto':
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
  if (newEx instanceof SqlRef) return newEx;

  return newEx.as((ex.getOutputName() || 'grouped').replace(/^[a-z]+_/i, ''));
}
