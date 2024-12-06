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
import { C, F, SqlColumn, SqlExpression, SqlQuery, SqlStar } from 'druid-query-toolkit';

import { filterMap, mapRecordIfChanged } from '../../../utils';

import { ExpressionMeta } from './expression-meta';
import { Measure } from './measure';
import type { ParameterDefinition, Parameters, ParameterValues } from './parameter';

function expressionWithinColumns(ex: SqlExpression, columns: readonly Column[]): boolean {
  const usedColumns = ex.getUsedColumnNames();
  return usedColumns.every(columnName => columns.some(c => c.name === columnName));
}

interface QuerySourceValue {
  query: SqlQuery;
  baseColumns: readonly Column[];
  columns: readonly Column[];
  measures: Measure[];
}

export class QuerySource {
  static isSimpleSelect(query: SqlQuery): boolean {
    return Boolean(
      !query.hasGroupBy() && !query.unionQuery && query.getFromExpressions().length === 1,
    );
  }

  static isSingleStarQuery(query: SqlQuery): boolean {
    const selectExpressions = query.getSelectExpressionsArray();
    return selectExpressions.length === 1 && selectExpressions[0] instanceof SqlStar;
  }

  static makeLimitZeroIntrospectionQuery(query: SqlQuery): SqlQuery {
    return SqlQuery.selectStarFrom(query).changeLimitValue(0);
  }

  static stripToBaseSource(query: SqlQuery): SqlQuery {
    if (query.hasGroupBy()) {
      if (!query.fromClause) throw new Error('must have FROM clause');
      return SqlQuery.create(query.fromClause);
    } else {
      return query.changeSelectExpressions([SqlStar.PLAIN]).changeLimitValue(undefined);
    }
  }

  static fromIntrospectResult(
    query: SqlQuery,
    baseColumns: readonly Column[],
    columns: readonly Column[],
  ): QuerySource {
    let effectiveColumns = columns;
    if (query.getSelectExpressionsArray().some(ex => ex instanceof SqlStar)) {
      // The query has a star so carefully pick the columns that make sense
      effectiveColumns = columns.filter(
        c => c.sqlType !== 'OTHER' || c.nativeType === 'COMPLEX<json>',
      );
    }

    let measures = Measure.extractQueryMeasures(query);
    if (!measures.length) {
      const countColumn = columns.find(c => c.name === 'count' || c.name === '__count');
      measures = [
        countColumn
          ? new Measure({
              expression: F.sum(C(countColumn.name)),
              as: 'Count',
            })
          : Measure.COUNT,
      ];

      measures = [
        ...measures,
        ...filterMap(columns, column =>
          column.nativeType?.startsWith('COMPLEX<')
            ? Measure.getPossibleMeasuresForColumn(column)[0]
            : undefined,
        ),
      ];
    }

    return new QuerySource({
      query,
      baseColumns,
      columns: effectiveColumns,
      measures,
    });
  }

  public readonly query: SqlQuery;
  public readonly baseColumns: readonly Column[];
  public readonly columns: readonly Column[];
  public readonly measures: Measure[];

  constructor(value: QuerySourceValue) {
    this.query = value.query;
    this.baseColumns = value.baseColumns;
    this.columns = value.columns;
    this.measures = value.measures;
  }

  public valueOf(): QuerySourceValue {
    return {
      query: this.query,
      baseColumns: this.baseColumns,
      columns: this.columns,
      measures: this.measures,
    };
  }

  public getInitQuery(where?: SqlExpression): SqlQuery {
    return SqlQuery.from(this.query.as('t')).changeWhereExpression(where);
  }

  public getInitBaseQuery(): SqlQuery {
    return SqlQuery.from(QuerySource.stripToBaseSource(this.query).as('t'));
  }

  private materializeStarIfNeeded(): SqlQuery {
    const { query, columns, measures } = this;
    let columnsToExpand = columns.map(c => c.name);
    const selectExpressions = query.getSelectExpressionsArray();
    let starCount = 0;
    for (const selectExpression of selectExpressions) {
      if (selectExpression instanceof SqlStar) {
        starCount++;
        continue;
      }
      const outputName = selectExpression.getOutputName();
      if (!outputName) continue;
      columnsToExpand = columnsToExpand.filter(c => c !== outputName);
    }
    if (starCount === 0) return query;
    if (starCount > 1) throw new Error('can not handle multiple stars');

    return Measure.addMeasuresToQuery(
      query
        .changeSelectExpressions(
          selectExpressions.flatMap(selectExpression =>
            selectExpression instanceof SqlStar ? columnsToExpand.map(c => C(c)) : selectExpression,
          ),
        )
        .prettify(),
      measures,
    );
  }

  public getFirstAggregateMeasure(): Measure | undefined {
    return this.measures[0]?.toAggregateBasedMeasure();
  }

  public getFirstAggregateMeasureArray(): Measure[] {
    return this.measures.length ? [this.measures[0].toAggregateBasedMeasure()] : [];
  }

  public getAvailableName(prefix: string): string {
    let columnName = prefix;
    let counter = 1;
    while (this.nameInUse(columnName)) {
      counter++;
      columnName = `${prefix}_${counter}`;
    }
    return columnName;
  }

  public nameInUse(nameToCheck: string): boolean {
    return this.hasColumnByName(nameToCheck) || this.hasMeasureByName(nameToCheck);
  }

  public hasColumnByName(name: string): boolean {
    return this.columns.some(c => c.name === name);
  }

  public hasMeasureByName(name: string): boolean {
    return this.measures.some(m => m.name === name);
  }

  public hasBaseTimeColumn(): boolean {
    return this.baseColumns.some(column => column.isTimeColumn());
  }

  public getSourceExpressionForColumn(outputName: string): SqlExpression {
    const selectExpressionsArray = this.query.getSelectExpressionsArray();

    const sourceExpression = selectExpressionsArray.find(ex => ex.getOutputName() === outputName);
    if (sourceExpression) return sourceExpression;

    const m = /^EXPR\$(\d+)$/.exec(outputName);
    if (m) {
      const index = parseInt(m[1], 10);
      if (selectExpressionsArray[index]) return selectExpressionsArray[index];
    }

    return C(outputName);
  }

  private getSourceToBaseSubstitutions(): Map<string, SqlExpression> {
    return new Map<string, SqlExpression>(
      filterMap(this.query.getSelectExpressionsArray(), ex => {
        const outputName = ex.getOutputName();
        const underlyingExpression = ex.getUnderlyingExpression();
        if (!outputName || underlyingExpression.getOutputName() === outputName) return;
        return [outputName, underlyingExpression];
      }),
    );
  }

  public transformToBaseColumns(expression: SqlExpression): SqlExpression {
    const sourceToBaseSubstitutions = this.getSourceToBaseSubstitutions();
    return expression.walk(ex => {
      if (ex instanceof SqlColumn) {
        return sourceToBaseSubstitutions.get(ex.getName()) || ex;
      }
      return ex;
    }) as SqlExpression;
  }

  public addWhereClause(clause: SqlExpression): SqlQuery {
    return this.query.addWhere(clause);
  }

  public addColumn(newExpression: SqlExpression): SqlQuery {
    const noStarQuery = this.materializeStarIfNeeded();
    return noStarQuery.addSelect(newExpression);
  }

  public addColumnAfter(neighborName: string, ...newExpressions: SqlExpression[]): SqlQuery {
    const noStarQuery = this.materializeStarIfNeeded();
    return noStarQuery.changeSelectExpressions(
      noStarQuery
        .getSelectExpressionsArray()
        .flatMap(ex => (ex.getOutputName() === neighborName ? [ex, ...newExpressions] : ex)),
    );
  }

  public changeColumn(oldName: string, newExpression: SqlExpression): SqlQuery {
    const noStarQuery = this.materializeStarIfNeeded();
    return noStarQuery.changeSelectExpressions(
      noStarQuery
        .getSelectExpressionsArray()
        .map(ex => (ex.getOutputName() === oldName ? newExpression : ex)),
    );
  }

  public deleteColumn(outputName: string): SqlQuery {
    const noStarQuery = this.materializeStarIfNeeded();
    return noStarQuery.changeSelectExpressions(
      noStarQuery.getSelectExpressionsArray().filter(ex => ex.getOutputName() !== outputName),
    );
  }

  public getColumnNameMap(nameTransform: (columnName: string) => string): Map<string, string> {
    return new Map(this.columns.map(column => [column.name, nameTransform(column.name)]));
  }

  public applyColumnNameMap(columnNameMap: Map<string, string>): SqlQuery {
    const noStarQuery = this.materializeStarIfNeeded();
    return noStarQuery.changeSelectExpressions(
      noStarQuery.getSelectExpressionsArray().map(ex => {
        const outputName = ex.getOutputName();
        if (!outputName) return ex;
        const newOutputName = columnNameMap.get(outputName);
        if (!newOutputName || newOutputName === outputName) return ex;
        return ex.as(newOutputName);
      }),
    );
  }

  // ------------------------------------

  public addMeasure(measure: Measure): SqlQuery {
    const noStarQuery = this.materializeStarIfNeeded();
    return Measure.addMeasuresToQuery(noStarQuery, this.measures.concat(measure));
  }

  public addMeasureAfter(neighborName: string, newMeasure: Measure): SqlQuery {
    const noStarQuery = this.materializeStarIfNeeded();
    return Measure.addMeasuresToQuery(
      noStarQuery,
      this.measures.flatMap(m => (m.name === neighborName ? [m, newMeasure] : m)),
    );
  }

  public changeMeasure(oldName: string, newMeasure: Measure): SqlQuery {
    const noStarQuery = this.materializeStarIfNeeded();
    return Measure.addMeasuresToQuery(
      noStarQuery,
      this.measures.map(m => (m.name === oldName ? newMeasure : m)),
    );
  }

  public deleteMeasure(measureName: string): SqlQuery {
    const noStarQuery = this.materializeStarIfNeeded();
    return Measure.addMeasuresToQuery(
      noStarQuery,
      this.measures.filter(m => m.name !== measureName),
    );
  }

  // --------------------------------

  public restrictWhere(where: SqlExpression): SqlExpression {
    const { columns } = this;
    const parts = where.decomposeViaAnd();
    const filterParts = parts.filter(ex => expressionWithinColumns(ex, columns));
    if (parts.length === filterParts.length) return where;
    return SqlExpression.and(...filterParts);
  }

  public restrictParameterValues(
    parameterValues: ParameterValues,
    parameters: Parameters,
  ): ParameterValues {
    return mapRecordIfChanged(parameterValues, (parameterValue, k) =>
      this.restrictParameterValue(parameterValue, parameters[k]),
    );
  }

  private restrictParameterValue(parameterValue: any, parameter: ParameterDefinition): any {
    if (typeof parameterValue !== 'undefined') {
      switch (parameter.type) {
        case 'expression':
          if (!this.validateExpressionMeta(parameterValue)) return;
          break;

        case 'measure':
          if (!this.validateMeasure(parameterValue)) return;
          break;

        case 'expressions': {
          const valid = parameterValue.filter((v: ExpressionMeta) =>
            this.validateExpressionMeta(v),
          );
          if (valid.length !== parameterValue.length) return valid;
          break;
        }

        case 'measures': {
          const valid = parameterValue.filter((v: Measure) => this.validateMeasure(v));
          if (valid.length !== parameterValue.length) return valid;
          break;
        }

        default:
          break;
      }
    }
    return parameterValue;
  }

  public validateExpressionMeta(e: ExpressionMeta | undefined): e is ExpressionMeta {
    if (!(e instanceof ExpressionMeta)) return false;
    return expressionWithinColumns(e.expression, this.columns);
  }

  public validateMeasure(m: Measure | undefined): m is Measure {
    if (!(m instanceof Measure)) return false;

    const usedAggregates = m.getUsedAggregates();
    if (usedAggregates.some(usedAggregate => !this.hasMeasureByName(usedAggregate))) return false;

    return expressionWithinColumns(m.expression, this.columns);
  }
}
