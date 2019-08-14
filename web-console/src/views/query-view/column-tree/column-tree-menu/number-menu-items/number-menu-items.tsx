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
import { Alias, FilterClause, StringType } from 'druid-query-toolkit';
import { aliasFactory } from 'druid-query-toolkit/build/ast/sql-query/helpers';
import React from 'react';

import { RowFilter } from '../../../query-view';

export interface NumberMenuItemsProps {
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
  hasGroupBy?: boolean;
  columnName: string;
}

export class NumberMenuItems extends React.PureComponent<NumberMenuItemsProps> {
  constructor(props: NumberMenuItemsProps, context: any) {
    super(props, context);
  }

  renderFilterMenu(): JSX.Element {
    const { columnName, filterByRow } = this.props;

    return (
      <MenuItem icon={IconNames.FILTER} text={`Filter by...`}>
        <MenuItem
          text={`"${columnName}" > 100`}
          onClick={() => filterByRow([{ row: 100, header: columnName, operator: '>' }], false)}
        />
        <MenuItem
          text={`"${columnName}" <= 100`}
          onClick={() => filterByRow([{ row: 100, header: columnName, operator: '<=' }], false)}
        />
      </MenuItem>
    );
  }

  renderGroupByMenu(): JSX.Element {
    const { columnName, addFunctionToGroupBy, addToGroupBy } = this.props;

    return (
      <MenuItem icon={IconNames.GROUP_OBJECTS} text={`Group by...`}>
        <MenuItem text={`"${columnName}"`} onClick={() => addToGroupBy(columnName, true)} />
        <MenuItem
          text={`TRUNCATE("${columnName}", 1) AS ${columnName}-truncated`}
          onClick={() =>
            addFunctionToGroupBy(
              'TRUNCATE',
              [' '],
              [
                new StringType({
                  spacing: [],
                  chars: columnName,
                  quote: '"',
                }),
                1,
              ],
              true,
              aliasFactory(`${columnName}-truncated`),
            )
          }
        />
      </MenuItem>
    );
  }

  renderAggregateMenu(): JSX.Element {
    const { columnName, addAggregateColumn } = this.props;
    return (
      <MenuItem icon={IconNames.FUNCTION} text={`Aggregate...`}>
        <MenuItem
          text={`SUM(${columnName}) AS "sum_${columnName}"`}
          onClick={() =>
            addAggregateColumn(columnName, 'SUM', true, aliasFactory(`sum_${columnName}`))
          }
        />
        <MenuItem
          text={`MAX(${columnName}) AS "max_${columnName}"`}
          onClick={() =>
            addAggregateColumn(columnName, 'MAX', true, aliasFactory(`max_${columnName}`))
          }
        />
        <MenuItem
          text={`MIN(${columnName}) AS "min_${columnName}"`}
          onClick={() =>
            addAggregateColumn(columnName, 'MIN', true, aliasFactory(`min_${columnName}`))
          }
        />
      </MenuItem>
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
