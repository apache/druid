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
  ComparisonExpression,
  ComparisonExpressionRhs,
  FilterClause,
  refExpressionFactory,
  SqlQuery,
  WhereClause,
} from 'druid-query-toolkit';
import { aliasFactory, stringFactory } from 'druid-query-toolkit/build/ast/sql-query/helpers';
import React from 'react';

export interface StringMenuItemsProps {
  columnName: string;
  parsedQuery: SqlQuery;
  onQueryChange: (queryString: SqlQuery, run?: boolean) => void;
}

export function StringMenuItems(props: StringMenuItemsProps) {
  function renderFilterMenu(): JSX.Element | undefined {
    const { columnName, parsedQuery, onQueryChange } = props;

    return (
      <MenuItem icon={IconNames.FILTER} text={`Filter`}>
        <MenuItem
          text={`"${columnName}" = 'xxx'`}
          onClick={() => {
            onQueryChange(parsedQuery.filterRow(columnName, 'xxx', '='), false);
          }}
        />
        <MenuItem
          text={`"${columnName}" LIKE '%xxx%'`}
          onClick={() => {
            onQueryChange(parsedQuery.filterRow(columnName, '%xxx%', 'LIKE'), false);
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
          text={`"${columnName}"`}
          onClick={() => {
            onQueryChange(parsedQuery.addToGroupBy(columnName), true);
          }}
        />
        <MenuItem
          text={`SUBSTRING("${columnName}", 1, 2) AS "${columnName}_substring"`}
          onClick={() => {
            onQueryChange(
              parsedQuery.addFunctionToGroupBy(
                'SUBSTRING',
                [' ', ' '],
                [stringFactory(columnName, `"`), 1, 2],

                aliasFactory(`${columnName}_substring`),
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
          text={`COUNT(DISTINCT "${columnName}") AS "dist_${columnName}"`}
          onClick={() =>
            onQueryChange(
              parsedQuery.addAggregateColumn(
                columnName,
                'COUNT',
                aliasFactory(`dist_${columnName}`),
              ),
              true,
            )
          }
        />
        <MenuItem
          text={`COUNT(*) FILTER (WHERE "${columnName}" = 'xxx') AS ${columnName}_filtered_count `}
          onClick={() => {
            onQueryChange(
              parsedQuery.addAggregateColumn(
                refExpressionFactory('*'),
                'COUNT',
                aliasFactory(`${columnName}_filtered_count`),
                false,
                new FilterClause({
                  keyword: 'FILTER',
                  spacing: [' '],
                  ex: new WhereClause({
                    keyword: 'WHERE',
                    spacing: [' '],
                    filter: new ComparisonExpression({
                      parens: [],
                      ex: stringFactory(columnName, '"'),
                      rhs: new ComparisonExpressionRhs({
                        parens: [],
                        op: '=',
                        rhs: stringFactory('xxx', `'`),
                        spacing: [' ', ' '],
                      }),
                    }),
                  }),
                }),
              ),
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
