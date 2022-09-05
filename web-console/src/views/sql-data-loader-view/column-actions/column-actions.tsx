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

import { Button, FormGroup, Menu, MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import { QueryResult, SqlExpression, SqlFunction } from 'druid-query-toolkit';
import React from 'react';

import { possibleDruidFormatForValues, TIME_COLUMN } from '../../../druid-models';
import { convertToGroupByExpression, oneOf, QueryAction, timeFormatToSql } from '../../../utils';
import { TimeFloorMenuItem } from '../../workbench-view/time-floor-menu-item/time-floor-menu-item';

import './column-actions.scss';

interface ColumnActionsProps {
  queryResult: QueryResult | undefined;
  headerIndex: number;
  onQueryAction(action: QueryAction): void;
}

export const ColumnActions = React.memo(function ExpressionEditor(props: ColumnActionsProps) {
  const { onQueryAction, queryResult, headerIndex } = props;

  const transformMenuItems: JSX.Element[] = [];
  let convertButton: JSX.Element | undefined;
  let removeFilterButton: JSX.Element | undefined;

  const sqlQuery = queryResult?.sqlQuery;
  if (queryResult && sqlQuery && headerIndex !== -1) {
    const column = queryResult.header[headerIndex];
    const header = column.name;
    const type = column.sqlType || column.nativeType;

    const expression = queryResult.sqlQuery?.getSelectExpressionForIndex(headerIndex);

    if (sqlQuery.getEffectiveWhereExpression().containsColumn(header)) {
      removeFilterButton = (
        <Button
          icon={IconNames.FILTER_REMOVE}
          text="Remove filter on this column"
          onClick={() => {
            onQueryAction(q =>
              q.changeWhereExpression(q.getWhereExpression()?.removeColumnFromAnd(header)),
            );
          }}
        />
      );
    }

    const grouped: boolean | undefined = sqlQuery.hasGroupBy()
      ? sqlQuery.isGroupedSelectIndex(headerIndex)
      : undefined;

    if (expression) {
      if (column.sqlType === 'TIMESTAMP') {
        transformMenuItems.push(
          <TimeFloorMenuItem
            key="time_floor"
            expression={expression}
            onChange={expression => {
              onQueryAction(q => q.changeSelect(headerIndex, expression));
            }}
          />,
        );

        if (!column.isTimeColumn()) {
          transformMenuItems.push(
            <MenuItem
              key="declare_time"
              icon={IconNames.TIME}
              text="Use as the primary time column"
              onClick={() => {
                onQueryAction(q =>
                  q.removeSelectIndex(headerIndex).addSelect(expression.as(TIME_COLUMN), {
                    insertIndex: 0,
                    addToGroupBy: q.hasGroupBy() ? 'start' : undefined,
                  }),
                );
              }}
            />,
          );
        }
      } else {
        // Not a time column -------------------------------------------
        const values = queryResult.rows.map(row => row[headerIndex]);
        const possibleDruidFormat = possibleDruidFormatForValues(values);
        const formatSql = possibleDruidFormat ? timeFormatToSql(possibleDruidFormat) : undefined;

        if (formatSql) {
          const newSelectExpression = formatSql.fillPlaceholders([
            expression.getUnderlyingExpression(),
          ]);

          transformMenuItems.push(
            <MenuItem
              key="parse_time"
              icon={IconNames.TIME}
              text={`Parse as '${possibleDruidFormat}'`}
              onClick={() => {
                const outputName = expression?.getOutputName();
                if (!outputName) return;
                onQueryAction(q => q.changeSelect(headerIndex, newSelectExpression.as(outputName)));
              }}
            />,
            <MenuItem
              key="parse_time_and_make_primary"
              icon={IconNames.TIME}
              text={`Parse as '${possibleDruidFormat}' and use as the primary time column`}
              onClick={() => {
                onQueryAction(q =>
                  q.removeSelectIndex(headerIndex).addSelect(newSelectExpression.as(TIME_COLUMN), {
                    insertIndex: 0,
                    addToGroupBy: q.hasGroupBy() ? 'start' : undefined,
                  }),
                );
              }}
            />,
          );
        }

        if (typeof grouped === 'boolean') {
          if (grouped) {
            const convertToAggregate = (aggregates: SqlExpression[]) => {
              onQueryAction(q =>
                q.removeOutputColumn(header).applyForEach(aggregates, (q, aggregate) =>
                  q.addSelect(aggregate, {
                    insertIndex: 'last',
                  }),
                ),
              );
            };

            const underlyingSelectExpression = expression.getUnderlyingExpression();

            convertButton = (
              <Popover2
                content={
                  <Menu>
                    <Menu>
                      {oneOf(type, 'LONG', 'FLOAT', 'DOUBLE', 'BIGINT') && (
                        <>
                          <MenuItem
                            text="Convert to SUM(...)"
                            onClick={() => {
                              convertToAggregate([
                                SqlFunction.simple('SUM', [underlyingSelectExpression]).as(
                                  `sum_${header}`,
                                ),
                              ]);
                            }}
                          />
                          <MenuItem
                            text="Convert to MIN(...)"
                            onClick={() => {
                              convertToAggregate([
                                SqlFunction.simple('MIN', [underlyingSelectExpression]).as(
                                  `min_${header}`,
                                ),
                              ]);
                            }}
                          />
                          <MenuItem
                            text="Convert to MAX(...)"
                            onClick={() => {
                              convertToAggregate([
                                SqlFunction.simple('MAX', [underlyingSelectExpression]).as(
                                  `max_${header}`,
                                ),
                              ]);
                            }}
                          />
                          <MenuItem
                            text="Convert to SUM(...), MIN(...), and MAX(...)"
                            onClick={() => {
                              convertToAggregate([
                                SqlFunction.simple('SUM', [underlyingSelectExpression]).as(
                                  `sum_${header}`,
                                ),
                                SqlFunction.simple('MIN', [underlyingSelectExpression]).as(
                                  `min_${header}`,
                                ),
                                SqlFunction.simple('MAX', [underlyingSelectExpression]).as(
                                  `max_${header}`,
                                ),
                              ]);
                            }}
                          />
                        </>
                      )}
                      <MenuItem
                        text="Convert to APPROX_COUNT_DISTINCT_DS_HLL(...)"
                        onClick={() => {
                          convertToAggregate([
                            SqlFunction.simple('APPROX_COUNT_DISTINCT_DS_HLL', [
                              underlyingSelectExpression,
                            ]).as(`unique_${header}`),
                          ]);
                        }}
                      />
                      <MenuItem
                        text="Convert to APPROX_COUNT_DISTINCT_DS_THETA(...)"
                        onClick={() => {
                          convertToAggregate([
                            SqlFunction.simple('APPROX_COUNT_DISTINCT_DS_THETA', [
                              underlyingSelectExpression,
                            ]).as(`unique_${header}`),
                          ]);
                        }}
                      />
                      <MenuItem
                        text="Convert to APPROX_COUNT_DISTINCT_BUILTIN(...)"
                        onClick={() => {
                          convertToAggregate([
                            SqlFunction.simple('APPROX_COUNT_DISTINCT_BUILTIN', [
                              underlyingSelectExpression,
                            ]).as(`unique_${header}`),
                          ]);
                        }}
                      />
                    </Menu>
                  </Menu>
                }
              >
                <Button icon={IconNames.EXCHANGE} text="Convert to metric" />
              </Popover2>
            );
          } else {
            const groupByExpression = convertToGroupByExpression(expression);
            if (groupByExpression) {
              convertButton = (
                <Button
                  icon={IconNames.EXCHANGE}
                  text="Convert to dimension"
                  onClick={() => {
                    onQueryAction(q =>
                      q.removeOutputColumn(header).addSelect(groupByExpression, {
                        insertIndex: 'last-grouping',
                        addToGroupBy: 'end',
                      }),
                    );
                  }}
                />
              );
            }
          }
        }
      }
    }
  }

  if (!transformMenuItems.length && !removeFilterButton && !convertButton) return null;

  return (
    <div className="column-actions">
      <div className="title">Column actions</div>
      {transformMenuItems.length > 0 && (
        <FormGroup>
          <Popover2 content={<Menu>{transformMenuItems}</Menu>}>
            <Button icon={IconNames.FUNCTION} text="Transform" />
          </Popover2>
        </FormGroup>
      )}
      {removeFilterButton && <FormGroup>{removeFilterButton}</FormGroup>}
      {convertButton && <FormGroup>{convertButton}</FormGroup>}
    </div>
  );
});
