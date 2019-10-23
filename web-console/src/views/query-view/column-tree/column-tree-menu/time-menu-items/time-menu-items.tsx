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
import { AdditiveExpression, SqlQuery, Timestamp, timestampFactory } from 'druid-query-toolkit';
import {
  aliasFactory,
  intervalFactory,
  refExpressionFactory,
  stringFactory,
} from 'druid-query-toolkit/build/ast/sql-query/helpers';
import React from 'react';

function dateToTimestamp(date: Date): Timestamp {
  return timestampFactory(
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
  columnName: string;
  parsedQuery: SqlQuery;
  onQueryChange: (queryString: SqlQuery, run?: boolean) => void;
}

export function TimeMenuItems(props: TimeMenuItemsProps) {
  function renderFilterMenu(): JSX.Element | undefined {
    const { columnName, parsedQuery, onQueryChange } = props;
    const now = new Date();

    return (
      <MenuItem icon={IconNames.FILTER} text={`Filter`}>
        <MenuItem
          text={`Latest hour`}
          onClick={() => {
            const additiveExpression = new AdditiveExpression({
              parens: [],
              op: ['-'],
              ex: [refExpressionFactory('CURRENT_TIMESTAMP'), intervalFactory('HOUR', '1')],
              spacing: [' ', ' '],
            });
            onQueryChange(
              parsedQuery.removeFilter(columnName).filterRow(columnName, additiveExpression, '>='),
              true,
            );
          }}
        />
        <MenuItem
          text={`Latest day`}
          onClick={() => {
            const additiveExpression = new AdditiveExpression({
              parens: [],
              op: ['-'],
              ex: [refExpressionFactory('CURRENT_TIMESTAMP'), intervalFactory('DAY', '1')],
              spacing: [' ', ' '],
            });
            onQueryChange(
              parsedQuery.removeFilter(columnName).filterRow(columnName, additiveExpression, '>='),
              true,
            );
          }}
        />
        <MenuItem
          text={`Latest week`}
          onClick={() => {
            const additiveExpression = new AdditiveExpression({
              parens: [],
              op: ['-'],
              ex: [refExpressionFactory('CURRENT_TIMESTAMP'), intervalFactory('DAY', '7')],
              spacing: [' ', ' '],
            });
            onQueryChange(
              parsedQuery.removeFilter(columnName).filterRow(columnName, additiveExpression, '>='),
              true,
            );
          }}
        />
        <MenuItem
          text={`Latest month`}
          onClick={() => {
            const additiveExpression = new AdditiveExpression({
              parens: [],
              op: ['-'],
              ex: [refExpressionFactory('CURRENT_TIMESTAMP'), intervalFactory('MONTH', '1')],
              spacing: [' ', ' '],
            });
            onQueryChange(
              parsedQuery.removeFilter(columnName).filterRow(columnName, additiveExpression, '>='),
              true,
            );
          }}
        />
        <MenuItem
          text={`Latest year`}
          onClick={() => {
            const additiveExpression = new AdditiveExpression({
              parens: [],
              op: ['-'],
              ex: [refExpressionFactory('CURRENT_TIMESTAMP'), intervalFactory('YEAR', '1')],
              spacing: [' ', ' '],
            });
            onQueryChange(
              parsedQuery.removeFilter(columnName).filterRow(columnName, additiveExpression, '>='),
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
                .filterRow(dateToTimestamp(hourStart), stringFactory(columnName, `"`), '<=')
                .filterRow(columnName, dateToTimestamp(nextHour(hourStart)), '<'),
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
                .filterRow(dateToTimestamp(dayStart), stringFactory(columnName, `"`), '<=')
                .filterRow(columnName, dateToTimestamp(nextDay(dayStart)), '<'),
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
                .filterRow(dateToTimestamp(monthStart), stringFactory(columnName, `"`), '<=')
                .filterRow(columnName, dateToTimestamp(nextMonth(monthStart)), '<'),
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
                .filterRow(dateToTimestamp(yearStart), stringFactory(columnName, `"`), '<=')
                .filterRow(columnName, dateToTimestamp(nextYear(yearStart)), '<'),
              true,
            );
          }}
        />
      </MenuItem>
    );
  }

  function renderRemoveFilter(): JSX.Element | undefined {
    const { columnName, parsedQuery, onQueryChange } = props;
    if (!parsedQuery.hasFilterForColumn(columnName)) return;

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
    if (!parsedQuery.hasGroupByForColumn(columnName)) return;
    return (
      <MenuItem
        icon={IconNames.UNGROUP_OBJECTS}
        text={'Remove group by'}
        onClick={() => {
          onQueryChange(parsedQuery.removeGroupBy(columnName), true);
        }}
      />
    );
  }

  function renderGroupByMenu(): JSX.Element | undefined {
    const { columnName, parsedQuery, onQueryChange } = props;
    if (!parsedQuery.hasGroupBy()) return;

    return (
      <MenuItem icon={IconNames.GROUP_OBJECTS} text={`Group by`}>
        <MenuItem
          text={`TIME_FLOOR("${columnName}", 'PT1H') AS "${columnName}_time_floor"`}
          onClick={() => {
            onQueryChange(
              parsedQuery.addFunctionToGroupBy(
                'TIME_FLOOR',
                [' '],
                [stringFactory(columnName, `"`), stringFactory('PT1H', `'`)],
                aliasFactory(`${columnName}_time_floor`),
              ),
              true,
            );
          }}
        />
        <MenuItem
          text={`TIME_FLOOR("${columnName}", 'P1D') AS "${columnName}_time_floor"`}
          onClick={() => {
            onQueryChange(
              parsedQuery.addFunctionToGroupBy(
                'TIME_FLOOR',
                [' '],
                [stringFactory(columnName, `"`), stringFactory('P1D', `'`)],
                aliasFactory(`${columnName}_time_floor`),
              ),
              true,
            );
          }}
        />
        <MenuItem
          text={`TIME_FLOOR("${columnName}", 'P7D') AS "${columnName}_time_floor"`}
          onClick={() => {
            onQueryChange(
              parsedQuery.addFunctionToGroupBy(
                'TIME_FLOOR',
                [' '],
                [stringFactory(columnName, `"`), stringFactory('P7D', `'`)],
                aliasFactory(`${columnName}_time_floor`),
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
    if (!parsedQuery.hasGroupBy()) return;

    return (
      <MenuItem icon={IconNames.FUNCTION} text={`Aggregate`}>
        <MenuItem
          text={`MAX("${columnName}") AS "max_${columnName}"`}
          onClick={() => {
            onQueryChange(
              parsedQuery.addAggregateColumn(columnName, 'MAX', aliasFactory(`max_${columnName}`)),
              true,
            );
          }}
        />
        <MenuItem
          text={`MIN("${columnName}") AS "min_${columnName}"`}
          onClick={() => {
            onQueryChange(
              parsedQuery.addAggregateColumn(columnName, 'MIN', aliasFactory(`min_${columnName}`)),
              true,
            );
          }}
        />
      </MenuItem>
    );
  }

  return (
    <>
      {renderFilterMenu()}
      {renderRemoveFilter()}
      {renderGroupByMenu()}
      {renderRemoveGroupBy()}
      {renderAggregateMenu()}
    </>
  );
}
