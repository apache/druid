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

import { MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { SqlExpression, SqlFunction, SqlLiteral, SqlQuery, SqlRef } from 'druid-query-toolkit';
import React from 'react';

import { prettyPrintSql } from '../../../../../utils';

const NINE_THOUSAND = SqlLiteral.create(9000);

export interface NumberMenuItemsProps {
  table: string;
  schema: string;
  columnName: string;
  parsedQuery: SqlQuery;
  onQueryChange: (query: SqlQuery, run?: boolean) => void;
}

export const NumberMenuItems = React.memo(function NumberMenuItems(props: NumberMenuItemsProps) {
  function renderFilterMenu(): JSX.Element {
    const { columnName, parsedQuery, onQueryChange } = props;
    const ref = SqlRef.column(columnName);

    function filterMenuItem(clause: SqlExpression) {
      return (
        <MenuItem
          text={prettyPrintSql(clause)}
          onClick={() => {
            onQueryChange(parsedQuery.addWhere(clause));
          }}
        />
      );
    }

    return (
      <MenuItem icon={IconNames.FILTER} text="Filter">
        {filterMenuItem(ref.greaterThan(NINE_THOUSAND))}
        {filterMenuItem(ref.lessThanOrEqual(NINE_THOUSAND))}
      </MenuItem>
    );
  }

  function renderRemoveFilter(): JSX.Element | undefined {
    const { columnName, parsedQuery, onQueryChange } = props;
    if (!parsedQuery.getEffectiveWhereExpression().containsColumn(columnName)) return;

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

  function renderGroupByMenu(): JSX.Element | undefined {
    const { columnName, parsedQuery, onQueryChange } = props;
    if (!parsedQuery.hasGroupBy()) return;
    const ref = SqlRef.column(columnName);

    function groupByMenuItem(ex: SqlExpression, alias?: string) {
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
        {groupByMenuItem(ref)}
        {groupByMenuItem(
          SqlFunction.simple('TRUNC', [ref, SqlLiteral.create(-1)]),
          `${columnName}_truncated`,
        )}
      </MenuItem>
    );
  }

  function renderRemoveGroupBy(): JSX.Element | undefined {
    const { columnName, parsedQuery, onQueryChange } = props;
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

  function renderAggregateMenu(): JSX.Element | undefined {
    const { columnName, parsedQuery, onQueryChange } = props;
    if (!parsedQuery.hasGroupBy()) return;
    const ref = SqlRef.column(columnName);

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
        {aggregateMenuItem(SqlFunction.simple('SUM', [ref]), `sum_${columnName}`)}
        {aggregateMenuItem(SqlFunction.simple('MIN', [ref]), `min_${columnName}`)}
        {aggregateMenuItem(SqlFunction.simple('MAX', [ref]), `max_${columnName}`)}
        {aggregateMenuItem(SqlFunction.simple('AVG', [ref]), `avg_${columnName}`)}
        {aggregateMenuItem(
          SqlFunction.simple('APPROX_QUANTILE', [ref, SqlLiteral.create(0.98)]),
          `p98_${columnName}`,
        )}
        {aggregateMenuItem(SqlFunction.simple('LATEST', [ref]), `latest_${columnName}`)}
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
