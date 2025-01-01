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
  F,
  SqlBetweenPart,
  SqlColumn,
  SqlComparison,
  SqlExpression,
  SqlFunction,
  SqlLiteral,
} from 'druid-query-toolkit';

import { partition } from '../../../utils';

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

export function computeWhereForCompares(
  where: SqlExpression,
  compares: Compare[],
  expandDuration: string | undefined,
): {
  commonWhere: SqlExpression;
  comparelessWhere: SqlExpression;
  mainWherePart: SqlExpression;
  perCompareWhereParts: SqlExpression[];
} {
  const whereParts = where.decomposeViaAnd({ flatten: true });
  const [timeExpressions, timelessExpressions] = partition(whereParts, expressionUsesTime);
  const comparelessWhere = SqlExpression.and(...timelessExpressions);
  const mainWherePart = SqlExpression.and(...timeExpressions);
  const perCompareWhereParts = compares.map(compare =>
    SqlExpression.and(
      ...timeExpressions.map(timeExpression =>
        shiftBackAndExpandTimeInExpression(timeExpression, compare, expandDuration),
      ),
    ),
  );
  return {
    commonWhere: SqlExpression.and(
      SqlExpression.or(mainWherePart, ...perCompareWhereParts),
      comparelessWhere,
    ),
    comparelessWhere,
    mainWherePart,
    perCompareWhereParts,
  };
}

function expressionUsesTime(expression: SqlExpression): boolean {
  return shiftBackAndExpandTimeInExpression(expression, 'P1D', undefined) !== expression;
}

export function decomposeTimeInInterval(expression: SqlExpression): SqlExpression {
  return expression.walk(ex => {
    if (ex instanceof SqlFunction && ex.getEffectiveFunctionName() === 'TIME_IN_INTERVAL') {
      // Ideally we could rewrite it to TIME_IN_INTERVAL(TIME_SHIFT(__time, period, 1), '<interval>') but that would be slow in the current Druid
      // return ex.changeArgs(ex.args!.change(0, F('TIME_SHIFT', ex.getArg(0), period, 1)));a

      const interval = ex.getArgAsString(1);
      if (!interval) return ex;

      const [start, end] = interval.split('/');
      if (!IS_DATE_LIKE.test(start) || !IS_DATE_LIKE.test(end)) return ex;

      const t = ex.getArg(0);
      if (!t) return ex;

      return isoStringToTimestampLiteral(start)
        .lessThanOrEqual(t)
        .and(t.lessThan(isoStringToTimestampLiteral(end)));
    }

    return ex;
  }) as SqlExpression;
}

export function shiftBackAndExpandTimeInExpression(
  expression: SqlExpression,
  compare: string,
  expandDuration: string | undefined,
): SqlExpression {
  return decomposeTimeInInterval(expression).walk(ex => {
    if (ex instanceof SqlComparison) {
      const { lhs, op, rhs } = ex;
      if (rhs instanceof SqlExpression) {
        if (op === '<' || op === '<=') {
          // TIMESTAMP '2022-01-02 03:04:05' <= __time --- start
          if (evaluatesToTimeLiteral(lhs) && rhs instanceof SqlColumn) {
            return ex.changeLhs(shiftBackAndExpandStart(lhs, compare, expandDuration));
          }

          // __time < TIMESTAMP '2022-01-02 03:04:05' --- end
          if (lhs instanceof SqlColumn && evaluatesToTimeLiteral(rhs)) {
            return ex.changeRhs(shiftBackAndExpandEnd(rhs, compare, expandDuration));
          }
        }

        if (op === '>' || op === '>=') {
          // __time > TIMESTAMP '2022-01-02 03:04:05' --- start
          if (lhs instanceof SqlColumn && evaluatesToTimeLiteral(rhs)) {
            return ex.changeRhs(shiftBackAndExpandStart(rhs, compare, expandDuration));
          }

          // TIMESTAMP '2022-01-02 03:04:05' >= __time --- end
          if (evaluatesToTimeLiteral(lhs) && rhs instanceof SqlColumn) {
            return ex.changeLhs(shiftBackAndExpandEnd(lhs, compare, expandDuration));
          }
        }
      } else if (rhs instanceof SqlBetweenPart) {
        const { start, end } = rhs;
        if (evaluatesToTimeLiteral(start) && evaluatesToTimeLiteral(end)) {
          return ex.changeRhs(
            rhs
              .changeStart(shiftBackAndExpandStart(start, compare, expandDuration))
              .changeEnd(shiftBackAndExpandEnd(end, compare, expandDuration)),
          );
        }
      }
    }

    return ex;
  }) as SqlExpression;
}

export function evaluatesToTimeLiteral(ex: SqlExpression): boolean {
  if (ex instanceof SqlLiteral) {
    // TIMESTAMP '2022-01-02 03:04:05'
    return ex.isDate();
  } else if (ex instanceof SqlFunction) {
    const effectiveFunctionName = ex.getEffectiveFunctionName();

    if (effectiveFunctionName === 'MAX_DATA_TIME') return true;
    if (effectiveFunctionName === 'TIME_SHIFT') {
      const arg0 = ex.getArg(0);
      return Boolean(arg0 && evaluatesToTimeLiteral(arg0));
    }
    if (effectiveFunctionName === 'TIME_PARSE') {
      const arg0 = ex.getArg(0);
      if (!arg0) return false;
      if (arg0 instanceof SqlLiteral) return true;
      return evaluatesToTimeLiteral(arg0);
    }
  }
  return false;
}

function shiftBackAndExpandStart(
  start: SqlExpression,
  shiftDuration: string,
  expandDuration: string | undefined,
): SqlExpression {
  const ex = F.timeShift(start, shiftDuration, -1);
  return expandDuration ? F.timeFloor(ex, expandDuration) : ex;
}

function shiftBackAndExpandEnd(
  end: SqlExpression,
  shiftDuration: string,
  expandDuration: string | undefined,
): SqlExpression {
  const ex = F.timeShift(end, shiftDuration, -1);
  return expandDuration ? F.timeCeil(ex, expandDuration) : ex;
}
