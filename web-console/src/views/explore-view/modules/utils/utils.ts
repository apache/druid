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

import { F, SqlExpression, SqlFunction, SqlLiteral } from '@druid-toolkit/query';

import { partition } from '../../../../utils';

const IS_DATE_LIKE = /^[+-]?\d\d\d\d[^']+$/;

export type Compare = `P${string}`;

function isoStringToTimestampLiteral(iso: string): SqlExpression {
  const zulu = iso.endsWith('Z');
  const cleanIso = iso.replace('T', ' ').replace('Z', '');
  let sql: string;
  if (zulu) {
    sql = `TIME_PARSE('${cleanIso}', NULL, 'Etc/UTC')`;
  } else {
    sql = `TIMESTAMP '${cleanIso}'`;
  }
  return SqlExpression.parse(sql);
}

export function decodeWhereForCompares(
  where: SqlExpression,
  compares: Compare[],
): {
  commonWhere: SqlExpression;
  mainWherePart: SqlExpression;
  perCompareWhereParts: SqlExpression[];
} {
  const whereParts = where.decomposeViaAnd({ flatten: true });
  const [timeExpressions, timelessExpressions] = partition(whereParts, expressionUsesTime);
  return {
    commonWhere: SqlExpression.and(...timelessExpressions),
    mainWherePart: SqlExpression.and(...timeExpressions),
    perCompareWhereParts: compares.map(compare =>
      SqlExpression.and(
        ...timeExpressions.map(timeExpression => shiftTimeInExpression(timeExpression, compare)),
      ),
    ),
  };
}

export function getWhereForCompares(where: SqlExpression, compares: Compare[]): SqlExpression {
  const { commonWhere, mainWherePart, perCompareWhereParts } = decodeWhereForCompares(
    where,
    compares,
  );

  return SqlExpression.and(SqlExpression.or(mainWherePart, ...perCompareWhereParts), commonWhere);
}

function expressionUsesTime(expression: SqlExpression): boolean {
  return shiftTimeInExpression(expression, 'P1D') !== expression;
}

export function shiftTimeInExpression(expression: SqlExpression, compare: string): SqlExpression {
  return expression.walk(ex => {
    if (ex instanceof SqlLiteral) {
      // Works with: __time < TIMESTAMP '2022-01-02 03:04:05'
      if (ex.isDate()) {
        return F.timeShift(ex, compare, -1);
      }
    } else if (ex instanceof SqlFunction) {
      const effectiveFunctionName = ex.getEffectiveFunctionName();

      // Works with: TIME_IN_INTERVAL(__time, '<interval>')
      if (effectiveFunctionName === 'TIME_IN_INTERVAL') {
        // Ideally we could rewrite it to TIME_IN_INTERVAL(TIME_SHIFT(__time, period, 1), '<interval>') but that would be slow in the current Druid
        // return ex.changeArgs(ex.args!.change(0, F('TIME_SHIFT', ex.getArg(0), period, 1)));a

        const interval = ex.getArgAsString(1);
        if (!interval) return ex;

        const [start, end] = interval.split('/');
        if (!IS_DATE_LIKE.test(start) || !IS_DATE_LIKE.test(end)) return ex;

        const t = ex.getArg(0);
        if (!t) return ex;

        return F.timeShift(isoStringToTimestampLiteral(start), compare, -1)
          .lessThanOrEqual(t)
          .and(t.lessThan(F.timeShift(isoStringToTimestampLiteral(end), compare, -1)));
      }

      // Works with: TIME_SHIFT(...) <= __time
      //        and: __time < MAX_DATA_TIME()
      if (effectiveFunctionName === 'TIME_SHIFT' || effectiveFunctionName === 'MAX_DATA_TIME') {
        return F.timeShift(ex, compare, -1);
      }
    }

    return ex;
  }) as SqlExpression;
}
