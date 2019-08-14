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
  Alias,
  ComparisonExpression,
  ComparisonExpressionRhs,
  FilterClause,
  SqlQuery,
  StringType,
  WhereClause,
} from 'druid-query-toolkit';
import { aliasFactory, stringFactory } from 'druid-query-toolkit/build/ast/sql-query/helpers';
import React from 'react';

import { RowFilter } from '../../../query-view';

export interface StringMenuItemsProps {
  addFunctionToGroupBy: (
    functionName: string,
    spacing: string[],
    argumentsArray: (StringType | number)[],
    run: boolean,
    alias: Alias,
  ) => void;
  addToGroupBy: (columnName: string, run: boolean) => void;
  addAggregateColumn: (
    columnName: string,
    functionName: string,
    run: boolean,
    alias?: Alias,
    distinct?: boolean,
    filter?: FilterClause,
  ) => void;
  filterByRow: (filters: RowFilter[], preferablyRun: boolean) => void;
  queryAst?: SqlQuery;
  columnName: string;
}

export class StringMenuItems extends React.PureComponent<StringMenuItemsProps> {
  constructor(props: StringMenuItemsProps, context: any) {
    super(props, context);
  }

  renderFilterMenu(): JSX.Element {
    const { columnName, filterByRow } = this.props;

    return (
      <MenuItem icon={IconNames.FILTER} text={`Filter`}>
        <MenuItem
          text={`"${columnName}" = 'xxx'`}
          onClick={() => filterByRow([{ row: 'xxx', header: columnName, operator: '=' }], false)}
        />
        <MenuItem
          text={`"${columnName}" LIKE '%xxx%'`}
          onClick={() => filterByRow([{ row: 'xxx', header: columnName, operator: 'LIKE' }], false)}
        />
      </MenuItem>
    );
  }

  renderGroupByMenu(): JSX.Element {
    const { columnName, addFunctionToGroupBy, addToGroupBy } = this.props;

    return (
      <MenuItem icon={IconNames.GROUP_OBJECTS} text={`Group by`}>
        <MenuItem text={`"${columnName}"`} onClick={() => addToGroupBy(columnName, true)} />
        <MenuItem
          text={`SUBSTRING("${columnName}", 1, 2) AS" "${columnName}_substring"`}
          onClick={() =>
            addFunctionToGroupBy(
              'SUBSTRING',
              [' ', ' '],
              [stringFactory(columnName, `"`), 1, 2],
              true,
              aliasFactory(`${columnName}_substring`),
            )
          }
        />
      </MenuItem>
    );
  }

  renderAggregateMenu(): JSX.Element {
    const { columnName, addAggregateColumn } = this.props;
    return (
      <MenuItem icon={IconNames.FUNCTION} text={`Aggregate`}>
        <MenuItem
          text={`COUNT(DISTINCT "${columnName}") AS "dist_${columnName}"`}
          onClick={() =>
            addAggregateColumn(columnName, 'COUNT', true, aliasFactory(`dist_${columnName}`), true)
          }
        />
        <MenuItem
          text={`COUNT(*) FILTER(WHERE "${columnName}" = 'xxx') `}
          onClick={() =>
            addAggregateColumn(
              '*',
              'COUNT',
              false,
              undefined,
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
            )
          }
        />
      </MenuItem>
    );
  }

  render(): JSX.Element {
    const { queryAst } = this.props;
    let hasGroupBy;
    if (queryAst) {
      hasGroupBy = queryAst.groupByClause;
    }
    return (
      <>
        {queryAst && this.renderFilterMenu()}
        {hasGroupBy && this.renderGroupByMenu()}
        {hasGroupBy && this.renderAggregateMenu()}
      </>
    );
  }
}
