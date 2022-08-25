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

import { Icon } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import { QueryResult, SqlExpression } from 'druid-query-toolkit';
import React, { useMemo } from 'react';

import { LearnMore, PopoverText } from '../../../../components';
import { getLink } from '../../../../links';
import { filterMap } from '../../../../utils';

import { ExpressionEntry } from './expression-entry/expression-entry';

import './column-list.scss';

export interface ColumnListProps {
  queryResult: QueryResult;
  columnFilter?(columnName: string): boolean;
  selectedColumnIndex: number;
  onEditColumn(columnIndex: number): void;
}

export const ColumnList = function ColumnList(props: ColumnListProps) {
  const { queryResult, columnFilter, selectedColumnIndex, onEditColumn } = props;

  const dimensions = useMemo(() => {
    const { sqlQuery } = queryResult;
    if (!sqlQuery) return [];
    return sqlQuery.hasGroupBy()
      ? sqlQuery.getGroupedSelectExpressions()
      : sqlQuery.getSelectExpressionsArray();
  }, [queryResult]);

  const metrics = useMemo(() => {
    const { sqlQuery } = queryResult;
    if (!sqlQuery) return;
    return sqlQuery.hasGroupBy() ? sqlQuery.getAggregateSelectExpressions() : undefined;
  }, [queryResult]);

  function getColumnIndexForExpression(ex: SqlExpression): number {
    if (!queryResult) return -1;
    const outputName = ex.getOutputName();
    return queryResult.header.findIndex(c => c.name === outputName);
  }

  return (
    <div className="column-list">
      <div className="list-column">
        <div className="list-label">
          {metrics ? (
            <>
              {'Dimensions '}
              <Popover2
                className="info-popover"
                content={
                  <PopoverText>
                    <p>
                      Dimension columns are stored as-is, so they can be filtered on, grouped by, or
                      aggregated at query time. They are always single Strings, arrays of Strings,
                      single Longs, single Doubles or single Floats.
                    </p>
                    <LearnMore href={`${getLink('DOCS')}/ingestion/schema-design.html`} />
                  </PopoverText>
                }
                position="left-bottom"
              >
                <Icon icon={IconNames.INFO_SIGN} size={14} />
              </Popover2>
            </>
          ) : (
            'Columns'
          )}
        </div>
        <div className="list-container">
          {filterMap(dimensions, (ex, i) => {
            const columnIndex = getColumnIndexForExpression(ex);
            const column = queryResult.header[columnIndex];
            if (columnFilter && !columnFilter(column.name)) return;

            return (
              <ExpressionEntry
                key={i}
                column={column}
                headerIndex={columnIndex}
                queryResult={queryResult}
                grouped={metrics ? true : undefined}
                selected={selectedColumnIndex === columnIndex}
                onEditColumn={onEditColumn}
              />
            );
          })}
        </div>
      </div>
      {metrics && (
        <div className="list-column">
          <div className="list-label">
            {'Metrics '}
            <Popover2
              className="info-popover"
              content={
                <PopoverText>
                  <p>
                    Metric columns are stored pre-aggregated, so they can only be aggregated at
                    query time (not filtered or grouped by). They are often stored as numbers
                    (integers or floats) but can also be stored as complex objects like HyperLogLog
                    sketches or approximate quantile sketches.
                  </p>
                  <LearnMore href={`${getLink('DOCS')}/ingestion/schema-design.html`} />
                </PopoverText>
              }
              position="left-bottom"
            >
              <Icon icon={IconNames.INFO_SIGN} size={14} />
            </Popover2>
          </div>
          <div className="list-container">
            {filterMap(metrics, (ex, i) => {
              const columnIndex = getColumnIndexForExpression(ex);
              const column = queryResult?.header[columnIndex];
              if (columnFilter && !columnFilter(column.name)) return;

              return (
                <ExpressionEntry
                  key={i}
                  column={column}
                  headerIndex={columnIndex}
                  queryResult={queryResult}
                  grouped={false}
                  selected={selectedColumnIndex === columnIndex}
                  onEditColumn={onEditColumn}
                />
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
};
