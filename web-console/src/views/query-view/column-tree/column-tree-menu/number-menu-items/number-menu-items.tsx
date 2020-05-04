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
  SqlAliasRef,
  SqlFunction,
  SqlLiteral,
  SqlMulti,
  SqlQuery,
  SqlRef,
} from 'druid-query-toolkit';
import React from 'react';

import { getCurrentColumns } from '../../column-tree';

export interface NumberMenuItemsProps {
  table: string;
  schema: string;
  columnName: string;
  parsedQuery: SqlQuery;
  onQueryChange: (queryString: SqlQuery, run?: boolean) => void;
}

export const NumberMenuItems = React.memo(function NumberMenuItems(props: NumberMenuItemsProps) {
  function renderFilterMenu(): JSX.Element {
    const { columnName, parsedQuery, onQueryChange } = props;

    return (
      <MenuItem icon={IconNames.FILTER} text={`Filter`}>
        <MenuItem
          text={`"${columnName}" > 100`}
          onClick={() => {
            onQueryChange(
              parsedQuery.addWhereFilter(
                SqlRef.fromStringWithDoubleQuotes(columnName),
                '>',
                SqlLiteral.fromInput(100),
              ),
            );
          }}
        />
        <MenuItem
          text={`"${columnName}" <= 100`}
          onClick={() => {
            onQueryChange(
              parsedQuery.addWhereFilter(
                SqlRef.fromStringWithDoubleQuotes(columnName),
                '<=',
                SqlLiteral.fromInput(100),
              ),
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
          text={`"${columnName}"`}
          onClick={() => {
            onQueryChange(
              parsedQuery.addToGroupBy(SqlRef.fromStringWithDoubleQuotes(columnName)),
              true,
            );
          }}
        />
        <MenuItem
          text={`TRUNC("${columnName}", -1) AS "${columnName}_trunc"`}
          onClick={() => {
            onQueryChange(
              parsedQuery.addToGroupBy(
                SqlAliasRef.sqlAliasFactory(
                  SqlFunction.sqlFunctionFactory('TRUNC', [
                    SqlRef.fromStringWithDoubleQuotes(columnName),
                    SqlLiteral.fromInput(-1),
                  ]),
                  `${columnName}_truncated`,
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
          text={`SUM(${columnName}) AS "sum_${columnName}"`}
          onClick={() => {
            onQueryChange(
              parsedQuery.addAggregateColumn(
                [SqlRef.fromString(columnName)],
                'SUM',
                `sum_${columnName}`,
              ),
              true,
            );
          }}
        />
        <MenuItem
          text={`MAX(${columnName}) AS "max_${columnName}"`}
          onClick={() => {
            onQueryChange(
              parsedQuery.addAggregateColumn(
                [SqlRef.fromString(columnName)],
                'MAX',
                `max_${columnName}`,
              ),
              true,
            );
          }}
        />
        <MenuItem
          text={`MIN(${columnName}) AS "min_${columnName}"`}
          onClick={() => {
            onQueryChange(
              parsedQuery.addAggregateColumn(
                [SqlRef.fromString(columnName)],
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
