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

import type { Column, SqlBase, SqlQuery } from 'druid-query-toolkit';
import { C, F, filterMap, L, SqlAlias, SqlExpression, SqlFunction } from 'druid-query-toolkit';

import { uniq } from '../../../utils';

import type { ExpressionMetaValue } from './expression-meta';
import { ExpressionMeta } from './expression-meta';
import { MeasurePattern } from './measure-pattern';

export interface MeasureValue extends ExpressionMetaValue {
  formatter?: (v: any) => string;
}

export class Measure extends ExpressionMeta {
  static AGGREGATE = 'AGGREGATE';
  static MAX_NAME_LENGTH = 100;
  static COUNT: Measure;

  static getAggregateMeasureName(expression: SqlBase): string | undefined {
    if (
      expression instanceof SqlFunction &&
      expression.getEffectiveFunctionName() === Measure.AGGREGATE
    ) {
      const arg0 = expression.getArgAsString(0);
      if (arg0) return arg0;
    }

    return;
  }

  static inflate(value: any): Measure | undefined {
    if (!value) return;

    const expression = SqlExpression.maybeParse(value.expression);
    if (!expression) return;

    return new Measure({ ...value, expression });
  }

  static inflateArray(value: any): Measure[] {
    if (!Array.isArray(value)) return [];
    return filterMap(value, Measure.inflate);
  }

  static columnToAggregateExpression(column: Column): SqlExpression | undefined {
    const { name, sqlType } = column;
    const c = C(name);
    switch (sqlType) {
      case 'BIGINT':
      case 'FLOAT':
      case 'DOUBLE':
        return F.sum(c);

      case 'VARCHAR':
        return F.countDistinct(c);

      default:
        return;
    }
  }

  static getPossibleMeasuresForColumn(column: Column): Measure[] {
    if (column.isTimeColumn()) {
      return [
        new Measure({
          expression: F.max(C(column.name)),
        }),
        new Measure({
          expression: F.min(C(column.name)),
        }),
      ];
    }

    switch (column.nativeType) {
      case 'LONG':
      case 'FLOAT':
      case 'DOUBLE':
        return [
          new Measure({
            expression: F.sum(C(column.name)),
          }),
          new Measure({
            expression: F.max(C(column.name)),
          }),
          new Measure({
            expression: F.min(C(column.name)),
          }),
          new Measure({
            as: `P98 ${column.name}`,
            expression: F('APPROX_QUANTILE_DS', C(column.name), 0.98),
          }),
          new Measure({
            expression: SqlFunction.countDistinct(C(column.name)),
          }),
        ];

      case 'STRING':
      case 'COMPLEX':
      case 'COMPLEX<hyperUnique>':
        return [
          new Measure({
            expression: SqlFunction.countDistinct(C(column.name)),
          }),
        ];

      case 'COMPLEX<HLLSketch>':
        return [
          new Measure({
            expression: F('APPROX_COUNT_DISTINCT_DS_HLL', C(column.name)),
          }),
        ];

      case 'COMPLEX<thetaSketch>':
        return [
          new Measure({
            expression: F('APPROX_COUNT_DISTINCT_DS_THETA', C(column.name)),
          }),
        ];

      case 'COMPLEX<quantilesDoublesSketch>':
        return [
          new Measure({
            as: `P98 ${column.name}`,
            expression: F('APPROX_QUANTILE_DS', C(column.name), 0.98),
          }),
          new Measure({
            as: `P95 ${column.name}`,
            expression: F('APPROX_QUANTILE_DS', C(column.name), 0.95),
          }),
          new Measure({
            as: `Median ${column.name}`,
            expression: F('APPROX_QUANTILE_DS', C(column.name), 0.5),
          }),
        ];

      default:
        return [];
    }
  }

  static extractQueryMeasures(query: SqlQuery): Measure[] {
    if (query.hasGroupBy()) return [];
    return filterMap(query.getSpace('preFromClause', '').split('\n'), line => {
      const m = /^\s*--:MEASURE\s+(.+)$/i.exec(line);
      if (!m) return;
      const ex = SqlExpression.maybeParse(m[1]);
      if (!(ex instanceof SqlAlias)) return;
      return new Measure({
        as: ex.getAliasName(),
        expression: ex.getUnderlyingExpression(),
      });
    });
  }

  static addMeasuresToQuery(query: SqlQuery, measures: Measure[]): SqlQuery {
    if (query.hasGroupBy()) throw new Error('can not addMeasure comments to a Group by query');
    return query.changeSpace(
      'preFromClause',
      '\n' +
        measures.map(measure => `  --:MEASURE ${measure.expression.as(measure.name)}`).join('\n') +
        '\n',
    );
  }

  static defaultNameFromExpression(expression: SqlExpression): string {
    const measurePattern = MeasurePattern.fit(expression);
    if (measurePattern) {
      return measurePattern.prettyPrint();
    }

    const aggregateMeasureName = Measure.getAggregateMeasureName(expression);
    if (aggregateMeasureName) return aggregateMeasureName;

    return ExpressionMeta.defaultNameFromExpression(expression);
  }

  public readonly formatter?: (v: any) => string;

  constructor(value: MeasureValue) {
    super(value);
    (this as any).name = this.as || Measure.defaultNameFromExpression(this.expression);
  }

  public equivalent(other: ExpressionMeta | undefined): boolean {
    if (!other || this.name !== other.name) return false;

    if (Measure.getAggregateMeasureName(this.expression) === other.name) {
      return true;
    }

    if (Measure.getAggregateMeasureName(other.expression) === this.name) {
      return true;
    }

    return this.expression.equals(other.expression);
  }

  public toAggregateBasedMeasure(): Measure {
    return this.changeExpression(F(Measure.AGGREGATE, L(this.name)));
  }

  public getAggregateMeasureName(): string | undefined {
    return Measure.getAggregateMeasureName(this.expression);
  }

  public getUsedAggregates(): string[] {
    return uniq(
      filterMap(
        this.expression.collect((ex): ex is SqlFunction =>
          Boolean(Measure.getAggregateMeasureName(ex)),
        ),
        ex => ex.getArgAsString(0),
      ),
    );
  }
}

Measure.COUNT = new Measure({
  expression: SqlFunction.COUNT_STAR,
});
