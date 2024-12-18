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

import type { Column } from 'druid-query-toolkit';
import { C, filterMap, SqlColumn, SqlExpression } from 'druid-query-toolkit';

import { renameColumnsInExpression } from '../utils';

export interface ExpressionMetaValue {
  expression: SqlExpression;
  as?: string;
}

export class ExpressionMeta {
  static MAX_NAME_LENGTH = 100;

  static inflate(value: any): ExpressionMeta | undefined {
    if (!value) return;

    const expression = SqlExpression.maybeParse(value.expression);
    if (!expression) return;

    return new ExpressionMeta({ ...value, expression });
  }

  static inflateArray(value: any): ExpressionMeta[] {
    if (!Array.isArray(value)) return [];
    return filterMap(value, ExpressionMeta.inflate);
  }

  static defaultNameFromExpression(expression: SqlExpression): string {
    if (expression instanceof SqlColumn) {
      return expression.getName();
    } else {
      return String(expression.prettyTrim(50));
    }
  }

  static fromColumn(column: Column): ExpressionMeta {
    return new ExpressionMeta({
      expression: C(column.name),
    });
  }

  public readonly expression: SqlExpression;
  public readonly as?: string;

  public readonly name: string;

  constructor(value: ExpressionMetaValue) {
    this.expression = value.expression;
    this.as = value.as ? value.as : undefined;
    this.name = this.as || ExpressionMeta.defaultNameFromExpression(this.expression);
  }

  public valueOf(): ExpressionMetaValue {
    return {
      expression: this.expression,
      as: this.as,
    };
  }

  public equals(other: ExpressionMeta | undefined): boolean {
    return Boolean(other && this.name === other.name && this.expression.equals(other.expression));
  }

  public change(newValues: Partial<ExpressionMetaValue>): this {
    return new (this.constructor as any)({
      ...this.valueOf(),
      ...newValues,
    });
  }

  public changeAs(as: string): this {
    return this.change({ as });
  }

  public changeExpression(expression: SqlExpression): this {
    return this.change({ expression });
  }

  public renameInExpression(rename: Map<string, string>): this {
    const renamedExpression = renameColumnsInExpression(this.expression, rename);
    if (renamedExpression === this.expression) return this;
    return this.changeExpression(renamedExpression);
  }
}
