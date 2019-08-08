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

import { Menu, MenuItem, Popover, Position } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import {
  AdditiveExpression,
  Alias,
  ComparisonExpression,
  ComparisonExpressionRhs,
  FilterClause,
  StringType,
  WhereClause,
} from 'druid-query-toolkit';
import { aliasFactory, stringFactory } from 'druid-query-toolkit/build/ast/sql-query/helpers';
import React from 'react';

export interface StringMenuProps {
  addFunctionToGroupBy: (
    functionName: string,
    spacing: string[],
    argumentsArray: (StringType | number)[],
    run: boolean,
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
  filterByRow: (
    rhs: string | number | AdditiveExpression,
    lhs: string,
    operator: '!=' | '=' | '>' | '<' | 'like' | '>=' | '<=' | 'LIKE',
    run: boolean,
  ) => void;
  hasGroupBy?: boolean;
  columnName: string;
}

export class StringMenu extends React.PureComponent<StringMenuProps> {
  constructor(props: StringMenuProps, context: any) {
    super(props, context);
  }

  renderFilterMenu(): JSX.Element {
    const { columnName, filterByRow } = this.props;

    return (
      <Popover
        boundary={'window'}
        position={Position.RIGHT}
        interactionKind={'hover'}
        fill
        content={
          <Menu>
            <MenuItem
              text={`"${columnName}" = 'xxx'`}
              onClick={() => filterByRow('xxx', columnName, '=', false)}
            />
            <MenuItem
              text={`"${columnName}" LIKE '%xxx%'`}
              onClick={() => filterByRow('%xxx%', columnName, 'LIKE', false)}
            />
          </Menu>
        }
      >
        <MenuItem icon={IconNames.FILTER} text={`Filter by...`} />
      </Popover>
    );
  }

  renderGroupByMenu(): JSX.Element {
    const { columnName, addFunctionToGroupBy, addToGroupBy } = this.props;

    return (
      <Popover
        boundary={'window'}
        position={Position.RIGHT}
        interactionKind={'hover'}
        fill
        content={
          <Menu>
            <MenuItem text={`"${columnName}"`} onClick={() => addToGroupBy(columnName, true)} />
            <MenuItem
              text={`SUBSTRING("${columnName}", 0, 2)`}
              onClick={() =>
                addFunctionToGroupBy(
                  'SUBSTRING',
                  ['', '', ' ', ' '],
                  [stringFactory(columnName, `"`), 0, 2],
                  false,
                )
              }
            />
          </Menu>
        }
      >
        <MenuItem icon={IconNames.GROUP_OBJECTS} text={`Group by...`} />
      </Popover>
    );
  }

  renderAggregateMenu(): JSX.Element {
    const { columnName, addAggregateColumn } = this.props;
    return (
      <Popover
        boundary={'window'}
        position={Position.RIGHT}
        interactionKind={'hover'}
        fill
        content={
          <Menu>
            <MenuItem
              text={`COUNT(DISTINCT "${columnName}") AS "dist_${columnName}"`}
              onClick={() =>
                addAggregateColumn(
                  columnName,
                  'COUNT',
                  true,
                  aliasFactory(`dist_${columnName}`),
                  true,
                )
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
          </Menu>
        }
      >
        <MenuItem icon={IconNames.FUNCTION} text={`Aggregate...`} />
      </Popover>
    );
  }

  render(): JSX.Element {
    const { hasGroupBy } = this.props;
    return (
      <>
        {this.renderFilterMenu()}
        {hasGroupBy && this.renderGroupByMenu()}
        {hasGroupBy && this.renderAggregateMenu()}
      </>
    );
  }
}
