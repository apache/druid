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
import { AdditiveExpression, Alias, FilterClause, StringType } from 'druid-query-toolkit';
import {
  aliasFactory,
  intervalFactory,
  refExpressionFactory,
  stringFactory,
} from 'druid-query-toolkit/build/ast/sql-query/helpers';
import React from 'react';

export interface TimeMenuProps {
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

export class TimeMenu extends React.PureComponent<TimeMenuProps> {
  constructor(props: TimeMenuProps, context: any) {
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
              text={`"${columnName}" >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR`}
              onClick={() => {
                const additiveExpression = new AdditiveExpression({
                  parens: [],
                  op: ['-'],
                  ex: [refExpressionFactory('CURRENT_TIMESTAMP'), intervalFactory('HOUR', '1')],
                  spacing: [' ', ' '],
                });
                filterByRow(additiveExpression, columnName, '>=', true);
              }}
            />
            <MenuItem
              text={`"${columnName}" >= CURRENT_TIMESTAMP - INTERVAL '1' DAY`}
              onClick={() => {
                const additiveExpression = new AdditiveExpression({
                  parens: [],
                  op: ['-'],
                  ex: [refExpressionFactory('CURRENT_TIMESTAMP'), intervalFactory('day', '1')],
                  spacing: [' ', ' '],
                });
                filterByRow(additiveExpression, columnName, '>=', true);
              }}
            />
            <MenuItem
              text={`"${columnName}" >= CURRENT_TIMESTAMP - INTERVAL '7' DAY`}
              onClick={() => {
                const additiveExpression = new AdditiveExpression({
                  parens: [],
                  op: ['-'],
                  ex: [refExpressionFactory('CURRENT_TIMESTAMP'), intervalFactory('day', '7')],
                  spacing: [' ', ' '],
                });
                filterByRow(additiveExpression, columnName, '>=', true);
              }}
            />
          </Menu>
        }
      >
        <MenuItem icon={IconNames.FILTER} text={`Filter by...`} />
      </Popover>
    );
  }

  renderGroupByMenu(): JSX.Element {
    const { columnName, addFunctionToGroupBy } = this.props;

    return (
      <Popover
        boundary={'window'}
        position={Position.RIGHT}
        interactionKind={'hover'}
        fill
        content={
          <Menu>
            <MenuItem
              text={`TIME_FLOOR("${columnName}", 'PT1H')`}
              onClick={() =>
                addFunctionToGroupBy(
                  'TIME_FLOOR',
                  ['', '', ' ', ' '],
                  [stringFactory(columnName, `"`), stringFactory('PT1H', `'`)],
                  true,
                )
              }
            />
            <MenuItem
              text={`TIME_FLOOR("${columnName}", 'P1D')`}
              onClick={() =>
                addFunctionToGroupBy(
                  'TIME_FLOOR',
                  ['', '', ' ', ' '],
                  [stringFactory(columnName, `"`), stringFactory('P1D', `'`)],
                  true,
                )
              }
            />
            <MenuItem
              text={`TIME_FLOOR("${columnName}", 'P7D')`}
              onClick={() =>
                addFunctionToGroupBy(
                  'TIME_FLOOR',
                  ['', '', ' ', ' '],
                  [stringFactory(columnName, `"`), stringFactory('P7D', `'`)],
                  true,
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
              text={`MAX("${columnName}") AS "max_${columnName}"`}
              onClick={() =>
                addAggregateColumn(columnName, 'MAX', true, aliasFactory(`max_${columnName}`))
              }
            />
            <MenuItem
              text={`MIN("${columnName}") AS "min_${columnName}"`}
              onClick={() =>
                addAggregateColumn(columnName, 'MIN', true, aliasFactory(`min_${columnName}`))
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
