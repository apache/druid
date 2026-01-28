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

import type { Column, SqlExpression } from 'druid-query-toolkit';
import { C, F, SqlColumn, SqlFunction, SqlStar } from 'druid-query-toolkit';

import { capitalizeFirst } from '../../../utils';

import { Measure } from './measure';

export type MeasurePatternAggregate = 'count' | 'sum' | 'min' | 'max' | 'countDistinct';

interface MeasurePatternValue {
  aggregate: MeasurePatternAggregate;
  column?: string;
}

export class MeasurePattern {
  static AGGREGATES: MeasurePatternAggregate[] = ['count', 'sum', 'min', 'max', 'countDistinct'];

  static fromColumn(column: Column): MeasurePattern {
    const ex = Measure.columnToAggregateExpression(column);
    if (!ex) return new MeasurePattern({ aggregate: 'countDistinct', column: column.name });
    return (
      MeasurePattern.fit(ex) ||
      new MeasurePattern({ aggregate: 'countDistinct', column: column.name })
    );
  }

  static fit(expression: SqlExpression): MeasurePattern | undefined {
    if (!(expression instanceof SqlFunction)) return;

    // Do not support filters for now
    if (expression.getWhereExpression()) return;

    const arg = expression.getArg(0);
    const functionName = expression.getEffectiveFunctionName();
    switch (functionName) {
      case 'COUNT':
        if (!expression.decorator && arg instanceof SqlStar)
          return new MeasurePattern({ aggregate: 'count' });
        if (
          expression.decorator !== 'DISTINCT' ||
          !(arg instanceof SqlColumn) ||
          arg.getTableName()
        )
          return;
        return new MeasurePattern({ aggregate: 'countDistinct', column: arg.getName() });

      case 'SUM':
      case 'MIN':
      case 'MAX':
        if (!(arg instanceof SqlColumn) || arg.getTableName()) return;
        return new MeasurePattern({
          aggregate: functionName.toLowerCase() as any,
          column: arg.getName(),
        });

      default:
        return;
    }
  }

  public readonly aggregate: MeasurePatternAggregate;
  public readonly column: string;

  constructor(value: MeasurePatternValue) {
    this.aggregate = value.aggregate;
    this.column = value.column || '*';
  }

  public valueOf(): MeasurePatternValue {
    return {
      aggregate: this.aggregate,
      column: this.column,
    };
  }

  public toString() {
    return String(this.toExpression());
  }

  public prettyPrint(): string {
    const { aggregate, column } = this;
    switch (aggregate) {
      case 'count':
        return 'Count';

      case 'countDistinct':
        return `Dist. ${column}`;

      case 'sum':
      case 'min':
      case 'max':
        return `${capitalizeFirst(aggregate)} ${column}`;

      default:
        throw new Error(`unknown aggregate ${aggregate}`);
    }
  }

  public changeAggregate(aggregate: MeasurePatternAggregate): MeasurePattern {
    const value = this.valueOf();
    value.aggregate = aggregate;
    if (aggregate === 'count') {
      value.column = '*';
    }
    return new MeasurePattern(value);
  }

  public changeColumn(column: string): MeasurePattern {
    return new MeasurePattern({ ...this.valueOf(), column });
  }

  public toExpression(): SqlExpression {
    const { aggregate, column } = this;
    switch (aggregate) {
      case 'count':
        return SqlFunction.COUNT_STAR;

      case 'countDistinct':
        return F.countDistinct(C(column));

      case 'sum':
      case 'min':
      case 'max':
        return F(aggregate.toUpperCase(), C(column));

      default:
        throw new Error(`unknown aggregate ${aggregate}`);
    }
  }
}
