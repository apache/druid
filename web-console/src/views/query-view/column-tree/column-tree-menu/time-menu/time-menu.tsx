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
  FilterClause,
  StringType,
  Timestamp,
  timeStampFactory,
} from 'druid-query-toolkit';
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
    rhs: string | number | AdditiveExpression | Timestamp,
    lhs: string | Timestamp,
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

  formatTime(timePart: number): string {
    if (timePart % 10 > 0) {
      return '0' + String(timePart);
    } else return String(timePart);
  }

  getNextMonth(month: number, year: number): { month: string; year: number } {
    if (month === 12) {
      return { month: '01', year: year + 1 };
    }
    return { month: this.formatTime(month + 1), year: year };
  }

  getNextDay(
    day: number,
    month: number,
    year: number,
  ): { day: string; month: string; year: number } {
    if (
      month === 1 ||
      month === 3 ||
      month === 5 ||
      month === 7 ||
      month === 8 ||
      month === 10 ||
      month === 12
    ) {
      if (day === 31) {
        const next = this.getNextMonth(month, year);
        return { day: '01', month: next.month, year: next.year };
      }
    } else if (month === 4 || month === 6 || month === 9 || month === 11) {
      if (day === 30) {
        const next = this.getNextMonth(month, year);
        return { day: '01', month: next.month, year: next.year };
      }
    } else if (month === 2) {
      if ((day === 29 && year % 4 === 0) || (day === 28 && year % 4)) {
        const next = this.getNextMonth(month, year);
        return { day: '01', month: next.month, year: next.year };
      }
    }
    return { day: this.formatTime(day + 1), month: this.formatTime(month), year: year };
  }

  getNextHour(
    hour: number,
    day: number,
    month: number,
    year: number,
  ): { hour: string; day: string; month: string; year: number } {
    if (hour === 23) {
      const next = this.getNextDay(day, month, year);
      return { hour: '00', day: next.day, month: next.month, year: next.year };
    }
    return {
      hour: this.formatTime(hour + 1),
      day: this.formatTime(day),
      month: this.formatTime(month),
      year: year,
    };
  }

  renderFilterMenu(): JSX.Element {
    const { columnName, filterByRow } = this.props;
    const date = new Date();
    const year = date.getFullYear();
    const month = date.getMonth();
    const day = date.getDay();
    const hour = date.getHours();

    return (
      <Popover
        boundary={'window'}
        position={Position.RIGHT}
        interactionKind={'hover'}
        fill
        content={
          <Menu>
            <MenuItem
              text={`Latest hour`}
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
              text={`Latest day`}
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
              text={`Latest week`}
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
            <MenuItem
              text={`Latest month`}
              onClick={() => {
                const additiveExpression = new AdditiveExpression({
                  parens: [],
                  op: ['-'],
                  ex: [refExpressionFactory('CURRENT_TIMESTAMP'), intervalFactory('month', '1')],
                  spacing: [' ', ' '],
                });
                filterByRow(additiveExpression, columnName, '>=', true);
              }}
            />
            <MenuItem
              text={`Latest year`}
              onClick={() => {
                const additiveExpression = new AdditiveExpression({
                  parens: [],
                  op: ['-'],
                  ex: [refExpressionFactory('CURRENT_TIMESTAMP'), intervalFactory('year', '1')],
                  spacing: [' ', ' '],
                });
                filterByRow(additiveExpression, columnName, '>=', true);
              }}
            />
            <MenuItem
              text={`Current hour`}
              onClick={() => {
                const next = this.getNextHour(hour, day, month, year);
                filterByRow(
                  columnName,
                  timeStampFactory(`${year}-${month}-${day} ${hour}:00:00`),
                  '<=',
                  false,
                );
                filterByRow(
                  timeStampFactory(`${next.year}-${next.month}-${next.day} ${next.hour}:00:00`),
                  columnName,
                  '<',
                  true,
                );
              }}
            />
            <MenuItem
              text={`Current day`}
              onClick={() => {
                const next = this.getNextDay(day, month, year);
                filterByRow(
                  columnName,
                  timeStampFactory(`${year}-${month}-${day} 00:00:00`),
                  '<=',
                  false,
                );
                filterByRow(
                  timeStampFactory(`${next.year}-${next.month}-${next.day} 00:00:00`),
                  columnName,
                  '<',
                  true,
                );
              }}
            />
            <MenuItem
              text={`Current month`}
              onClick={() => {
                const next = this.getNextMonth(month, year);
                filterByRow(
                  columnName,
                  timeStampFactory(`${year}-${month}-01 00:00:00`),
                  '<=',
                  false,
                );
                filterByRow(
                  timeStampFactory(`${next.year}-${next.month}-01 00:00:00`),
                  columnName,
                  '<',
                  true,
                );
              }}
            />
            <MenuItem
              text={`Current year`}
              onClick={() => {
                filterByRow(columnName, timeStampFactory(`${year}-01-01 00:00:00`), '<=', false);
                filterByRow(
                  timeStampFactory(`${Number(year) + 1}-01-01 00:00:00`),
                  columnName,
                  '<',
                  true,
                );
              }}
            />
            <MenuItem
              text={`test`}
              onClick={() => {
                filterByRow(timeStampFactory('2018-08-08 00:00:00'), 'name', '=', false);
                filterByRow('name', timeStampFactory('2018-08-08 00:00:00'), '>=', true);
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
