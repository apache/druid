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
import type { SqlQuery } from '@druid-toolkit/query';
import { C, F, SqlExpression } from '@druid-toolkit/query';
import type { JSX } from 'react';
import React from 'react';

import { prettyPrintSql } from '../../../../../utils';

const LATEST_HOUR: SqlExpression = SqlExpression.parse(
  `? >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`,
);
const LATEST_DAY: SqlExpression = SqlExpression.parse(`? >= CURRENT_TIMESTAMP - INTERVAL '1' DAY`);
const LATEST_WEEK: SqlExpression = SqlExpression.parse(
  `? >= CURRENT_TIMESTAMP - INTERVAL '1' WEEK`,
);
const LATEST_MONTH: SqlExpression = SqlExpression.parse(
  `? >= CURRENT_TIMESTAMP - INTERVAL '1' MONTH`,
);
const LATEST_YEAR: SqlExpression = SqlExpression.parse(
  `? >= CURRENT_TIMESTAMP - INTERVAL '1' YEAR`,
);

const BETWEEN: SqlExpression = SqlExpression.parse(`(? <= ? AND ? < ?)`);

// ------------------------------------

function fillWithColumn(b: SqlExpression, columnName: string): SqlExpression {
  return b.fillPlaceholders([C(columnName)]);
}

function fillWithColumnStartEnd(columnName: string, start: Date, end: Date): SqlExpression {
  const column = C(columnName);
  return BETWEEN.fillPlaceholders([start, column, column, end])!;
}

// ------------------------------------

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
  onQueryChange: (query: SqlQuery, run?: boolean) => void;
}

export const TimeMenuItems = React.memo(function TimeMenuItems(props: TimeMenuItemsProps) {
  const { columnName, parsedQuery, onQueryChange } = props;
  const column = C(columnName);

  function renderFilterMenu(): JSX.Element | undefined {
    function filterMenuItem(label: string, clause: SqlExpression) {
      return (
        <MenuItem
          text={label}
          onClick={() => {
            onQueryChange(parsedQuery.removeColumnFromWhere(columnName).addWhere(clause), true);
          }}
        />
      );
    }

    const now = new Date();
    const hourStart = floorHour(now);
    const dayStart = floorDay(now);
    const monthStart = floorMonth(now);
    const yearStart = floorYear(now);
    return (
      <MenuItem icon={IconNames.FILTER} text="Filter">
        {filterMenuItem(`Latest hour`, fillWithColumn(LATEST_HOUR, columnName))}
        {filterMenuItem(`Latest day`, fillWithColumn(LATEST_DAY, columnName))}
        {filterMenuItem(`Latest week`, fillWithColumn(LATEST_WEEK, columnName))}
        {filterMenuItem(`Latest month`, fillWithColumn(LATEST_MONTH, columnName))}
        {filterMenuItem(`Latest year`, fillWithColumn(LATEST_YEAR, columnName))}
        <MenuDivider />
        {filterMenuItem(
          `Current hour`,
          fillWithColumnStartEnd(columnName, hourStart, nextHour(hourStart)),
        )}
        {filterMenuItem(
          `Current day`,
          fillWithColumnStartEnd(columnName, dayStart, nextDay(dayStart)),
        )}
        {filterMenuItem(
          `Current month`,
          fillWithColumnStartEnd(columnName, monthStart, nextMonth(monthStart)),
        )}
        {filterMenuItem(
          `Current year`,
          fillWithColumnStartEnd(columnName, yearStart, nextYear(yearStart)),
        )}
      </MenuItem>
    );
  }

  function renderRemoveFilter(): JSX.Element | undefined {
    if (!parsedQuery.getEffectiveWhereExpression().containsColumnName(columnName)) return;

    return (
      <MenuItem
        icon={IconNames.FILTER_REMOVE}
        text="Remove filter"
        onClick={() => {
          onQueryChange(parsedQuery.removeColumnFromWhere(columnName), true);
        }}
      />
    );
  }

  function renderRemoveGroupBy(): JSX.Element | undefined {
    const groupedSelectIndexes = parsedQuery.getGroupedSelectIndexesForColumn(columnName);
    if (!groupedSelectIndexes.length) return;

    return (
      <MenuItem
        icon={IconNames.UNGROUP_OBJECTS}
        text="Remove group by"
        onClick={() => {
          onQueryChange(parsedQuery.removeSelectIndexes(groupedSelectIndexes), true);
        }}
      />
    );
  }

  function renderGroupByMenu(): JSX.Element | undefined {
    if (!parsedQuery.hasGroupBy()) return;

    function groupByMenuItem(ex: SqlExpression, alias: string) {
      return (
        <MenuItem
          text={prettyPrintSql(ex)}
          onClick={() => {
            onQueryChange(
              parsedQuery.addSelect(ex.as(alias), {
                insertIndex: 'last-grouping',
                addToGroupBy: 'end',
              }),
              true,
            );
          }}
        />
      );
    }

    return (
      <MenuItem icon={IconNames.GROUP_OBJECTS} text="Group by">
        {groupByMenuItem(F.timeFloor(column, 'PT1H'), `${columnName}_by_hour`)}
        {groupByMenuItem(F.timeFloor(column, 'P1D'), `${columnName}_by_day`)}
        {groupByMenuItem(F.timeFloor(column, 'P1M'), `${columnName}_by_month`)}
        {groupByMenuItem(F.timeFloor(column, 'P1Y'), `${columnName}_by_year`)}
        <MenuDivider />
        {groupByMenuItem(F('TIME_EXTRACT', column, 'HOUR'), `hour_of_${columnName}`)}
        {groupByMenuItem(F('TIME_EXTRACT', column, 'DAY'), `day_of_${columnName}`)}
        {groupByMenuItem(F('TIME_EXTRACT', column, 'MONTH'), `month_of_${columnName}`)}
        {groupByMenuItem(F('TIME_EXTRACT', column, 'YEAR'), `year_of_${columnName}`)}
      </MenuItem>
    );
  }

  function renderAggregateMenu(): JSX.Element | undefined {
    if (!parsedQuery.hasGroupBy()) return;

    function aggregateMenuItem(ex: SqlExpression, alias: string) {
      return (
        <MenuItem
          text={prettyPrintSql(ex)}
          onClick={() => {
            onQueryChange(parsedQuery.addSelect(ex.as(alias)), true);
          }}
        />
      );
    }

    return (
      <MenuItem icon={IconNames.FUNCTION} text="Aggregate">
        {aggregateMenuItem(F.max(column), `max_${columnName}`)}
        {aggregateMenuItem(F.min(column), `min_${columnName}`)}
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
});
