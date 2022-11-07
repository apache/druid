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
import {
  SqlExpression,
  SqlFunction,
  SqlJoinPart,
  SqlLiteral,
  SqlQuery,
  SqlRef,
  SqlTableRef,
} from 'druid-query-toolkit';
import React from 'react';

import { EMPTY_LITERAL, prettyPrintSql } from '../../../../../utils';
import { getJoinColumns } from '../../column-tree';

export interface StringMenuItemsProps {
  schema: string;
  table: string;
  columnName: string;
  parsedQuery: SqlQuery;
  onQueryChange: (query: SqlQuery, run?: boolean) => void;
}

export const StringMenuItems = React.memo(function StringMenuItems(props: StringMenuItemsProps) {
  function renderFilterMenu(): JSX.Element | undefined {
    const { columnName, parsedQuery, onQueryChange } = props;
    const ref = SqlRef.column(columnName);

    function filterMenuItem(clause: SqlExpression, run = true) {
      return (
        <MenuItem
          text={prettyPrintSql(clause)}
          onClick={() => {
            onQueryChange(parsedQuery.addWhere(clause), run);
          }}
        />
      );
    }

    return (
      <MenuItem icon={IconNames.FILTER} text="Filter">
        {filterMenuItem(ref.isNotNull())}
        {filterMenuItem(ref.equal(EMPTY_LITERAL), false)}
        {filterMenuItem(ref.like(EMPTY_LITERAL), false)}
        {filterMenuItem(SqlFunction.simple('REGEXP_LIKE', [ref, EMPTY_LITERAL]), false)}
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

  function renderGroupByMenu(): JSX.Element | undefined {
    const { columnName, parsedQuery, onQueryChange } = props;
    if (!parsedQuery.hasGroupBy()) return;

    function groupByMenuItem(ex: SqlExpression, alias?: string) {
      return (
        <MenuItem
          text={prettyPrintSql(ex)}
          onClick={() => {
            onQueryChange(
              parsedQuery.addSelect(alias ? ex.as(alias) : ex, {
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
        {groupByMenuItem(SqlRef.column(columnName))}
        {groupByMenuItem(
          SqlFunction.simple('SUBSTRING', [
            SqlRef.column(columnName),
            SqlLiteral.create(1),
            SqlLiteral.create(2),
          ]),
          `${columnName}_substring`,
        )}
        {groupByMenuItem(
          SqlFunction.simple('REGEXP_EXTRACT', [
            SqlRef.column(columnName),
            SqlLiteral.create('(\\d+)'),
          ]),
          `${columnName}_extract`,
        )}
      </MenuItem>
    );
  }

  function renderAggregateMenu(): JSX.Element | undefined {
    const { columnName, parsedQuery, onQueryChange } = props;
    if (!parsedQuery.hasGroupBy()) return;
    const ref = SqlRef.column(columnName);

    function aggregateMenuItem(ex: SqlExpression, alias: string, run = true) {
      return (
        <MenuItem
          text={prettyPrintSql(ex)}
          onClick={() => {
            onQueryChange(parsedQuery.addSelect(ex.as(alias)), run);
          }}
        />
      );
    }

    return (
      <MenuItem icon={IconNames.FUNCTION} text="Aggregate">
        {aggregateMenuItem(SqlFunction.countDistinct(ref), `dist_${columnName}`)}
        {aggregateMenuItem(
          SqlFunction.COUNT_STAR.addWhereExpression(ref.equal(EMPTY_LITERAL)),
          `filtered_dist_${columnName}`,
          false,
        )}
        {aggregateMenuItem(
          SqlFunction.simple('LATEST', [ref, SqlLiteral.create(100)]),
          `latest_${columnName}`,
        )}
      </MenuItem>
    );
  }

  function renderJoinMenu(): JSX.Element | undefined {
    const { schema, table, columnName, parsedQuery, onQueryChange } = props;
    if (schema !== 'lookup' || !parsedQuery) return;

    const { originalTableColumn, lookupColumn } = getJoinColumns(parsedQuery, table);

    return (
      <MenuItem icon={IconNames.JOIN_TABLE} text={parsedQuery.hasJoin() ? `Replace join` : `Join`}>
        <MenuItem
          icon={IconNames.LEFT_JOIN}
          text="Left join"
          onClick={() => {
            onQueryChange(
              parsedQuery.addJoin(
                SqlJoinPart.create(
                  'LEFT',
                  SqlTableRef.create(table, schema),
                  SqlRef.column(columnName, table, 'lookup').equal(
                    SqlRef.column(
                      lookupColumn === columnName ? originalTableColumn : 'XXX',
                      parsedQuery.getFirstTableName(),
                    ),
                  ),
                ),
              ),
              false,
            );
          }}
        />
        <MenuItem
          icon={IconNames.INNER_JOIN}
          text="Inner join"
          onClick={() => {
            onQueryChange(
              parsedQuery.addJoin(
                SqlJoinPart.create(
                  'INNER',
                  SqlTableRef.create(table, schema),
                  SqlRef.column(columnName, table, 'lookup').equal(
                    SqlRef.column(
                      lookupColumn === columnName ? originalTableColumn : 'XXX',
                      parsedQuery.getFirstTableName(),
                    ),
                  ),
                ),
              ),
              false,
            );
          }}
        />
      </MenuItem>
    );
  }

  function renderRemoveJoin(): JSX.Element | undefined {
    const { schema, parsedQuery, onQueryChange } = props;
    if (schema !== 'lookup' || !parsedQuery) return;
    if (!parsedQuery.hasJoin()) return;

    return (
      <MenuItem
        icon={IconNames.EXCHANGE}
        text="Remove join"
        onClick={() => onQueryChange(parsedQuery.removeAllJoins())}
      />
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
      {renderRemoveJoin()}
    </>
  );
});
