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
import {
  filterPatternToExpression,
  SqlExpression,
  SqlLiteral,
  SqlQuery,
} from 'druid-query-toolkit';

import { isEmpty } from '../../utils';

import type { Measure, ParameterValues } from './models';
import {
  ExpressionMeta,
  inflateParameterValues,
  QuerySource,
  renameColumnsInParameterValues,
} from './models';
import { ModuleRepository } from './module-repository/module-repository';
import type { Rename } from './utils';
import { renameColumnsInExpression } from './utils';

interface ExploreStateValue {
  source: string;
  showSourceQuery?: boolean;
  where: SqlExpression;
  moduleId: string;
  parameterValues: ParameterValues;
}

export class ExploreState {
  static DEFAULT_STATE: ExploreState;

  static fromJS(js: any) {
    const inflatedParameterValues = inflateParameterValues(
      js.parameterValues,
      ModuleRepository.getModule(js.moduleId)?.parameters || {},
    );
    return new ExploreState({
      ...js,
      where: SqlExpression.maybeParse(js.where) || SqlLiteral.TRUE,
      parameterValues: inflatedParameterValues,
    });
  }

  public readonly source: string;
  public readonly showSourceQuery: boolean;
  public readonly where: SqlExpression;
  public readonly moduleId: string;
  public readonly parameterValues: ParameterValues;

  public readonly parsedSource: SqlQuery | undefined;
  public readonly parseError: string | undefined;

  constructor(value: ExploreStateValue) {
    this.source = value.source;
    this.showSourceQuery = Boolean(value.showSourceQuery);
    this.where = value.where;
    this.moduleId = value.moduleId;
    this.parameterValues = value.parameterValues;

    if (this.source === '') {
      this.parseError = 'Please select source or enter a source query';
    } else {
      try {
        this.parsedSource = SqlQuery.parse(this.source);
      } catch (e) {
        this.parseError = e.message;
      }
    }
  }

  valueOf(): ExploreStateValue {
    return {
      source: this.source,
      showSourceQuery: this.showSourceQuery,
      where: this.where,
      moduleId: this.moduleId,
      parameterValues: this.parameterValues,
    };
  }

  public change(newValues: Partial<ExploreStateValue>): ExploreState {
    return new ExploreState({
      ...this.valueOf(),
      ...newValues,
    });
  }

  public changeSource(newSource: SqlQuery | string, rename: Rename | undefined): ExploreState {
    const toChange: Partial<ExploreStateValue> = {
      source: String(newSource),
    };

    if (rename) {
      toChange.where = renameColumnsInExpression(this.where, rename);

      const module = ModuleRepository.getModule(this.moduleId);
      if (module) {
        toChange.parameterValues = renameColumnsInParameterValues(
          this.parameterValues,
          module.parameters,
          rename,
        );
      }
    }

    return this.change(toChange);
  }

  public changeToTable(tableName: string): ExploreState {
    return this.changeSource(SqlQuery.create(tableName), undefined);
  }

  public addInitTimeFilterIfNeeded(columns: readonly Column[]): ExploreState {
    if (!this.parsedSource) return this;
    if (!QuerySource.isSingleStarQuery(this.parsedSource)) return this; // Only trigger for `SELECT * FROM ...` queries
    if (!this.where.equal(SqlLiteral.TRUE)) return this;

    // Either find the `__time::TIMESTAMP` column or use the first column if it is a TIMESTAMP
    const timeColumn =
      columns.find(c => c.isTimeColumn()) ||
      (columns[0].sqlType === 'TIMESTAMP' ? columns[0] : undefined);
    if (!timeColumn) return this;

    return this.change({
      where: filterPatternToExpression({
        type: 'timeRelative',
        column: timeColumn.name,
        negated: false,
        anchor: 'maxDataTime',
        rangeDuration: 'P1D',
        startBound: '[',
        endBound: ')',
      }),
    });
  }

  public restrictToQuerySource(querySource: QuerySource): ExploreState {
    const { where, moduleId, parameterValues } = this;
    const module = ModuleRepository.getModule(moduleId);
    if (!module) return this;
    const newWhere = querySource.restrictWhere(where);
    const newParameterValues = querySource.restrictParameterValues(
      parameterValues,
      module.parameters,
    );
    if (where === newWhere && parameterValues === newParameterValues) return this;

    return this.change({
      where: newWhere,
      parameterValues: newParameterValues,
    });
  }

  public applyShowColumn(column: Column): ExploreState {
    let moduleId: string;
    let parameterValues: ParameterValues;
    if (column.sqlType === 'TIMESTAMP') {
      moduleId = 'time-chart';
      parameterValues = {};
    } else {
      moduleId = 'grouping-table';
      parameterValues = {
        ...(this.moduleId === moduleId ? this.parameterValues : {}),
        splitColumns: [ExpressionMeta.fromColumn(column)],
      };
    }

    return this.change({
      moduleId,
      parameterValues,
    });
  }

  public applyShowMeasure(measure: Measure): ExploreState {
    const module = ModuleRepository.getModule(this.moduleId);
    if (module) {
      const p = Object.entries(module.parameters).find(
        ([_, def]) => def.type === 'measure' || def.type === 'measures',
      );
      if (p) {
        const [paramName, def] = p;
        const { parameterValues } = this;
        return this.change({
          parameterValues: {
            ...parameterValues,
            [paramName]:
              def.type === 'measures'
                ? (parameterValues[paramName] || []).concat(measure)
                : measure,
          },
        });
      }
    }

    return this.change({
      moduleId: 'grouping-table',
      parameterValues: {
        measures: [measure],
      },
    });
  }

  public isInitState(): boolean {
    return (
      this.moduleId === 'record-table' &&
      this.source === '' &&
      this.where instanceof SqlLiteral &&
      isEmpty(this.parameterValues)
    );
  }
}

ExploreState.DEFAULT_STATE = new ExploreState({
  moduleId: 'record-table',
  source: '',
  where: SqlLiteral.TRUE,
  parameterValues: {},
});
