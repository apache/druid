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

import { MenuDivider, MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import {
  SqlAliasRef,
  SqlFunction,
  SqlInterval,
  SqlLiteral,
  SqlMulti,
  SqlQuery,
  SqlRef,
  SqlTimestamp,
} from 'druid-query-toolkit';
import React from 'react';

import { getCurrentColumns } from '../../column-tree';

function dateToTimestamp(date: Date): SqlTimestamp {
  return SqlTimestamp.sqlTimestampFactory(
    date
      .toISOString()
      .split('.')[0]
      .split('T')
      .join(' '),
  );
}

function floorHour(dt: Date): Date {
  dt = new Date(dt.valueOf());
  dt.setUTCMinutes(0, 0, 0);
  return dt;
}

function nextHour(dt: Date): Date {
  dt = new Date(dt.valueOf());
  dt.setUTCHours(dt.getUTCHours() + 1);
  return dt;
}

function floorDay(dt: Date): Date {
  dt = new Date(dt.valueOf());
  dt.setUTCHours(0, 0, 0, 0);
  return dt;
}

function nextDay(dt: Date): Date {
  dt = new Date(dt.valueOf());
  dt.setUTCDate(dt.getUTCDate() + 1);
  return dt;
}

function floorMonth(dt: Date): Date {
  dt = new Date(dt.valueOf());
  dt.setUTCHours(0, 0, 0, 0);
  dt.setUTCDate(1);
  return dt;
}

function nextMonth(dt: Date): Date {
  dt = new Date(dt.valueOf());
  dt.setUTCMonth(dt.getUTCMonth() + 1);
  return dt;
}

function floorYear(dt: Date): Date {
  dt = new Date(dt.valueOf());
  dt.setUTCHours(0, 0, 0, 0);
  dt.setUTCMonth(0, 1);
  return dt;
}

function nextYear(dt: Date): Date {
  dt = new Date(dt.valueOf());
  dt.setUTCFullYear(dt.getUTCFullYear() + 1);
  return dt;
}

export interface TimeMenuItemsProps {
  table: string;
  schema: string;
  columnName: string;
  parsedQuery: SqlQuery;
  onQueryChange: (queryString: SqlQuery, run?: boolean) => void;
}

export const TimeMenuItems = React.memo(function TimeMenuItems(props: TimeMenuItemsProps) {
  function renderFilterMenu(): JSX.Element | undefined {
    const { columnName, parsedQuery, onQueryChange } = props;
    const now = new Date();

    return (
      <MenuItem icon={IconNames.FILTER} text={`Filter`}>
        <MenuItem
          text={`Latest hour`}
          onClick={() => {
            onQueryChange(
              parsedQuery
                .removeFilter(columnName)
                .addWhereFilter(
                  columnName,
                  '>=',
                  SqlMulti.sqlMultiFactory('-', [
                    SqlRef.fromString('CURRENT_TIMESTAMP'),
                    SqlInterval.sqlIntervalFactory('HOUR', 1),
                  ]),
                ),
              true,
            );
          }}
        />
        <MenuItem
          text={`Latest day`}
          onClick={() => {
            onQueryChange(
              parsedQuery
                .removeFilter(columnName)
                .addWhereFilter(
                  columnName,
                  '>=',
                  SqlMulti.sqlMultiFactory('-', [
                    SqlRef.fromString('CURRENT_TIMESTAMP'),
                    SqlInterval.sqlIntervalFactory('Day', 1),
                  ]),
                ),
              true,
            );
          }}
        />
        <MenuItem
          text={`Latest week`}
          onClick={() => {
            onQueryChange(
              parsedQuery
                .removeFilter(columnName)
                .addWhereFilter(
                  columnName,
                  '>=',
                  SqlMulti.sqlMultiFactory('-', [
                    SqlRef.fromString('CURRENT_TIMESTAMP'),
                    SqlInterval.sqlIntervalFactory('Day', 7),
                  ]),
                ),
              true,
            );
          }}
        />
        <MenuItem
          text={`Latest month`}
          onClick={() => {
            onQueryChange(
              parsedQuery
                .removeFilter(columnName)
                .addWhereFilter(
                  columnName,
                  '>=',
                  SqlMulti.sqlMultiFactory('-', [
                    SqlRef.fromString('CURRENT_TIMESTAMP'),
                    SqlInterval.sqlIntervalFactory('MONTH', 1),
                  ]),
                ),
              true,
            );
          }}
        />
        <MenuItem
          text={`Latest year`}
          onClick={() => {
            onQueryChange(
              parsedQuery
                .removeFilter(columnName)
                .addWhereFilter(
                  columnName,
                  '>=',
                  SqlMulti.sqlMultiFactory('-', [
                    SqlRef.fromString('CURRENT_TIMESTAMP'),
                    SqlInterval.sqlIntervalFactory('YEAR', 1),
                  ]),
                ),
              true,
            );
          }}
        />
        <MenuDivider />
        <MenuItem
          text={`Current hour`}
          onClick={() => {
            const hourStart = floorHour(now);
            onQueryChange(
              parsedQuery
                .removeFilter(columnName)
                .addWhereFilter(
                  SqlRef.fromStringWithDoubleQuotes(columnName),
                  '>=',
                  dateToTimestamp(hourStart),
                )
                .addWhereFilter(
                  SqlRef.fromStringWithDoubleQuotes(columnName),
                  '<',
                  dateToTimestamp(nextHour(hourStart)),
                ),
              true,
            );
          }}
        />
        <MenuItem
          text={`Current day`}
          onClick={() => {
            const dayStart = floorDay(now);
            onQueryChange(
              parsedQuery
                .removeFilter(columnName)
                .addWhereFilter(
                  SqlRef.fromStringWithDoubleQuotes(columnName),
                  '>=',
                  dateToTimestamp(dayStart),
                )
                .addWhereFilter(
                  SqlRef.fromStringWithDoubleQuotes(columnName),
                  '<',
                  dateToTimestamp(nextDay(dayStart)),
                ),
              true,
            );
          }}
        />
        <MenuItem
          text={`Current month`}
          onClick={() => {
            const monthStart = floorMonth(now);
            onQueryChange(
              parsedQuery
                .removeFilter(columnName)
                .addWhereFilter(
                  SqlRef.fromStringWithDoubleQuotes(columnName),
                  '>=',
                  dateToTimestamp(monthStart),
                )
                .addWhereFilter(
                  SqlRef.fromStringWithDoubleQuotes(columnName),
                  '<',
                  dateToTimestamp(nextMonth(monthStart)),
                ),
              true,
            );
          }}
        />
        <MenuItem
          text={`Current year`}
          onClick={() => {
            const yearStart = floorYear(now);
            onQueryChange(
              parsedQuery
                .removeFilter(columnName)
                .addWhereFilter(
                  SqlRef.fromStringWithDoubleQuotes(columnName),
                  '<=',
                  dateToTimestamp(yearStart),
                )
                .addWhereFilter(
                  dateToTimestamp(yearStart),
                  '<',
                  dateToTimestamp(nextYear(yearStart)),
                ),
              true,
            );
          }}
        />
      </MenuItem>
    );
  }

  function renderRemoveFilter(): JSX.Element | undefined {
    const { columnName, parsedQuery, onQueryChange } = props;
    if (!parsedQuery.getCurrentFilters().includes(columnName)) return;

    return (
      <MenuItem
        icon={IconNames.FILTER_REMOVE}
        text={`Remove filter`}
        onClick={() => {
          onQueryChange(parsedQuery.removeFilter(columnName), true);
        }}
      />
    );
  }

  function renderRemoveGroupBy(): JSX.Element | undefined {
    const { columnName, parsedQuery, onQueryChange } = props;
    if (!parsedQuery.hasGroupByColumn(columnName)) return;
    return (
      <MenuItem
        icon={IconNames.UNGROUP_OBJECTS}
        text={'Remove group by'}
        onClick={() => {
          onQueryChange(parsedQuery.removeFromGroupBy(columnName), true);
        }}
      />
    );
  }

  function renderGroupByMenu(): JSX.Element | undefined {
    const { columnName, parsedQuery, onQueryChange } = props;
    if (!parsedQuery.groupByExpression) return;

    return (
      <MenuItem icon={IconNames.GROUP_OBJECTS} text={`Group by`}>
        <MenuItem
          text={`TIME_FLOOR("${columnName}", 'PT1H') AS "${columnName}_time_floor"`}
          onClick={() => {
            onQueryChange(
              parsedQuery.addToGroupBy(
                SqlAliasRef.sqlAliasFactory(
                  SqlFunction.sqlFunctionFactory('TIME_FLOOR', [
                    SqlRef.fromString(columnName),
                    SqlLiteral.fromInput('PT1h'),
                  ]),
                  `${columnName}_time_floor`,
                ),
              ),
              true,
            );
          }}
        />
        <MenuItem
          text={`TIME_FLOOR("${columnName}", 'P1D') AS "${columnName}_time_floor"`}
          onClick={() => {
            onQueryChange(
              parsedQuery.addToGroupBy(
                SqlAliasRef.sqlAliasFactory(
                  SqlFunction.sqlFunctionFactory('TIME_FLOOR', [
                    SqlRef.fromString(columnName),
                    SqlLiteral.fromInput('P1D'),
                  ]),
                  `${columnName}_time_floor`,
                ),
              ),
              true,
            );
          }}
        />
        <MenuItem
          text={`TIME_FLOOR("${columnName}", 'P7D') AS "${columnName}_time_floor"`}
          onClick={() => {
            onQueryChange(
              parsedQuery.addToGroupBy(
                SqlAliasRef.sqlAliasFactory(
                  SqlFunction.sqlFunctionFactory('TIME_FLOOR', [
                    SqlRef.fromString(columnName),
                    SqlLiteral.fromInput('P7D'),
                  ]),
                  `${columnName}_time_floor`,
                ),
              ),
              true,
            );
          }}
        />
      </MenuItem>
    );
  }

  function renderAggregateMenu(): JSX.Element | undefined {
    const { columnName, parsedQuery, onQueryChange } = props;
    if (!parsedQuery.groupByExpression) return;

    return (
      <MenuItem icon={IconNames.FUNCTION} text={`Aggregate`}>
        <MenuItem
          text={`MAX("${columnName}") AS "max_${columnName}"`}
          onClick={() => {
            onQueryChange(
              parsedQuery.addAggregateColumn(
                [SqlRef.fromStringWithDoubleQuotes(columnName)],
                'MAX',
                `max_${columnName}`,
              ),
              true,
            );
          }}
        />
        <MenuItem
          text={`MIN("${columnName}") AS "min_${columnName}"`}
          onClick={() => {
            onQueryChange(
              parsedQuery.addAggregateColumn(
                [SqlRef.fromStringWithDoubleQuotes(columnName)],
                'MIN',
                `min_${columnName}`,
              ),
              true,
            );
          }}
        />
      </MenuItem>
    );
  }

  function renderJoinMenu(): JSX.Element | undefined {
    const { schema, table, columnName, parsedQuery, onQueryChange } = props;
    if (schema !== 'lookup' || !parsedQuery) return;

    const { originalTableColumn, lookupColumn } = getCurrentColumns(parsedQuery, table);

    return (
      <>
        <MenuItem
          icon={IconNames.JOIN_TABLE}
          text={parsedQuery.joinTable ? `Replace join` : `Join`}
        >
          <MenuItem
            icon={IconNames.LEFT_JOIN}
            text={`Left join`}
            onClick={() => {
              onQueryChange(
                parsedQuery.addJoin(
                  'LEFT',
                  SqlRef.fromString(table, schema).upgrade(),
                  SqlMulti.sqlMultiFactory('=', [
                    SqlRef.fromString(columnName, table, 'lookup'),
                    SqlRef.fromString(
                      lookupColumn === columnName ? originalTableColumn : 'XXX',
                      parsedQuery.getTableName(),
                    ),
                  ]),
                ),
                false,
              );
            }}
          />
          <MenuItem
            icon={IconNames.INNER_JOIN}
            text={`Inner join`}
            onClick={() => {
              onQueryChange(
                parsedQuery.addJoin(
                  'INNER',
                  SqlRef.fromString(table, schema).upgrade(),
                  SqlMulti.sqlMultiFactory('=', [
                    SqlRef.fromString(columnName, table, 'lookup'),
                    SqlRef.fromString(
                      lookupColumn === columnName ? originalTableColumn : 'XXX',
                      parsedQuery.getTableName(),
                    ),
                  ]),
                ),
                false,
              );
            }}
          />
        </MenuItem>
        {parsedQuery.onExpression &&
          parsedQuery.onExpression instanceof SqlMulti &&
          parsedQuery.onExpression.containsColumn(columnName) && (
            <MenuItem
              icon={IconNames.EXCHANGE}
              text={`Remove join`}
              onClick={() => onQueryChange(parsedQuery.removeJoin())}
            />
          )}
      </>
    );
  }

  return (
    <>
      {renderFilterMenu()}
      {renderRemoveFilter()}
      {renderGroupByMenu()}
      {renderRemoveGroupBy()}
      {renderAggregateMenu()}
      {renderJoinMenu()}
    </>
  );
});
