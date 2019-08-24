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
import { AdditiveExpression, SqlQuery, timestampFactory } from 'druid-query-toolkit';
import {
  aliasFactory,
  intervalFactory,
  refExpressionFactory,
  stringFactory,
} from 'druid-query-toolkit/build/ast/sql-query/helpers';
import React from 'react';

export interface TimeMenuItemsProps {
  columnName: string;
  parsedQuery: SqlQuery;
  onQueryChange: (queryString: SqlQuery, run?: boolean) => void;
}

export class TimeMenuItems extends React.PureComponent<TimeMenuItemsProps> {
  constructor(props: TimeMenuItemsProps, context: any) {
    super(props, context);
  }

  formatTime(timePart: number): string {
    if (timePart % 10 > 0) {
      return String(timePart);
    } else return '0' + String(timePart);
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

  renderFilterMenu(): JSX.Element | undefined {
    const { columnName, parsedQuery, onQueryChange } = this.props;

    const date = new Date();
    const year = date.getFullYear();
    const month = date.getMonth();
    const day = date.getDay();
    const hour = date.getHours();

    return (
      <MenuItem icon={IconNames.FILTER} text={`Filter`}>
        <MenuItem
          text={`Latest hour`}
          onClick={() => {
            const additiveExpression = new AdditiveExpression({
              parens: [],
              op: ['-'],
              ex: [refExpressionFactory('CURRENT_TIMESTAMP'), intervalFactory('HOUR', '1')],
              spacing: [' ', ' '],
            });
            onQueryChange(
              parsedQuery.removeFilter(columnName).filterRow(columnName, additiveExpression, '>='),
              true,
            );
          }}
        />
        <MenuItem
          text={`Latest day`}
          onClick={() => {
            const additiveExpression = new AdditiveExpression({
              parens: [],
              op: ['-'],
              ex: [refExpressionFactory('CURRENT_TIMESTAMP'), intervalFactory('DAY', '1')],
              spacing: [' ', ' '],
            });
            onQueryChange(
              parsedQuery.removeFilter(columnName).filterRow(columnName, additiveExpression, '>='),
              true,
            );
          }}
        />
        <MenuItem
          text={`Latest week`}
          onClick={() => {
            const additiveExpression = new AdditiveExpression({
              parens: [],
              op: ['-'],
              ex: [refExpressionFactory('CURRENT_TIMESTAMP'), intervalFactory('DAY', '7')],
              spacing: [' ', ' '],
            });
            onQueryChange(
              parsedQuery.removeFilter(columnName).filterRow(columnName, additiveExpression, '>='),
              true,
            );
          }}
        />
        <MenuItem
          text={`Latest month`}
          onClick={() => {
            const additiveExpression = new AdditiveExpression({
              parens: [],
              op: ['-'],
              ex: [refExpressionFactory('CURRENT_TIMESTAMP'), intervalFactory('MONTH', '1')],
              spacing: [' ', ' '],
            });
            onQueryChange(
              parsedQuery.removeFilter(columnName).filterRow(columnName, additiveExpression, '>='),
              true,
            );
          }}
        />
        <MenuItem
          text={`Latest year`}
          onClick={() => {
            const additiveExpression = new AdditiveExpression({
              parens: [],
              op: ['-'],
              ex: [refExpressionFactory('CURRENT_TIMESTAMP'), intervalFactory('YEAR', '1')],
              spacing: [' ', ' '],
            });
            onQueryChange(
              parsedQuery.removeFilter(columnName).filterRow(columnName, additiveExpression, '>='),
              true,
            );
          }}
        />
        <MenuDivider />
        <MenuItem
          text={`Current hour`}
          onClick={() => {
            const next = this.getNextHour(hour, day, month, year);
            onQueryChange(
              parsedQuery
                .removeFilter(columnName)
                .filterRow(
                  timestampFactory(`${year}-${month}-${day} ${this.formatTime(hour)}:00:00`),
                  stringFactory(columnName, `"`),
                  '<=',
                )
                .filterRow(
                  columnName,
                  timestampFactory(`${next.year}-${next.month}-${next.day} ${next.hour}:00:00`),
                  '<',
                ),
              true,
            );
          }}
        />
        <MenuItem
          text={`Current day`}
          onClick={() => {
            const next = this.getNextDay(day, month, year);
            onQueryChange(
              parsedQuery
                .removeFilter(columnName)
                .filterRow(
                  timestampFactory(`${year}-${month}-${day} 00:00:00`),
                  stringFactory(columnName, `"`),
                  '<=',
                )
                .filterRow(
                  columnName,
                  timestampFactory(`${next.year}-${next.month}-${next.day} 00:00:00`),
                  '<',
                ),
              true,
            );
          }}
        />
        <MenuItem
          text={`Current month`}
          onClick={() => {
            const next = this.getNextMonth(month, year);
            onQueryChange(
              parsedQuery
                .removeFilter(columnName)
                .filterRow(
                  timestampFactory(`${year}-${month}-01 00:00:00`),
                  stringFactory(columnName, `"`),
                  '<=',
                )
                .filterRow(
                  columnName,
                  timestampFactory(`${next.year}-${next.month}-01 00:00:00`),
                  '<',
                ),
              true,
            );
          }}
        />
        <MenuItem
          text={`Current year`}
          onClick={() => {
            onQueryChange(
              parsedQuery
                .removeFilter(columnName)
                .filterRow(
                  timestampFactory(`${year}-01-01 00:00:00`),
                  stringFactory(columnName, `"`),
                  '<=',
                )
                .filterRow(columnName, timestampFactory(`${Number(year) + 1}-01-01 00:00:00`), '<'),
              true,
            );
          }}
        />
      </MenuItem>
    );
  }

  renderRemoveFilter(): JSX.Element | undefined {
    const { columnName, parsedQuery, onQueryChange } = this.props;
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

  renderGroupByMenu(): JSX.Element | undefined {
    const { columnName, parsedQuery, onQueryChange } = this.props;
    if (!parsedQuery.hasGroupBy()) return;

    return (
      <MenuItem icon={IconNames.GROUP_OBJECTS} text={`Group by`}>
        <MenuItem
          text={`TIME_FLOOR("${columnName}", 'PT1H') AS "${columnName}_time_floor"`}
          onClick={() => {
            onQueryChange(
              parsedQuery.addFunctionToGroupBy(
                'TIME_FLOOR',
                [' '],
                [stringFactory(columnName, `"`), stringFactory('PT1H', `'`)],
                aliasFactory(`${columnName}_time_floor`),
              ),
              true,
            );
          }}
        />
        <MenuItem
          text={`TIME_FLOOR("${columnName}", 'P1D') AS "${columnName}_time_floor"`}
          onClick={() => {
            onQueryChange(
              parsedQuery.addFunctionToGroupBy(
                'TIME_FLOOR',
                [' '],
                [stringFactory(columnName, `"`), stringFactory('P1D', `'`)],
                aliasFactory(`${columnName}_time_floor`),
              ),
              true,
            );
          }}
        />
        <MenuItem
          text={`TIME_FLOOR("${columnName}", 'P7D') AS "${columnName}_time_floor"`}
          onClick={() => {
            onQueryChange(
              parsedQuery.addFunctionToGroupBy(
                'TIME_FLOOR',
                [' '],
                [stringFactory(columnName, `"`), stringFactory('P7D', `'`)],
                aliasFactory(`${columnName}_time_floor`),
              ),
              true,
            );
          }}
        />
      </MenuItem>
    );
  }

  renderAggregateMenu(): JSX.Element | undefined {
    const { columnName, parsedQuery, onQueryChange } = this.props;
    if (!parsedQuery.hasGroupBy()) return;

    return (
      <MenuItem icon={IconNames.FUNCTION} text={`Aggregate`}>
        <MenuItem
          text={`MAX("${columnName}") AS "max_${columnName}"`}
          onClick={() => {
            onQueryChange(
              parsedQuery.addAggregateColumn(columnName, 'MAX', aliasFactory(`max_${columnName}`)),
              true,
            );
          }}
        />
        <MenuItem
          text={`MIN("${columnName}") AS "min_${columnName}"`}
          onClick={() => {
            onQueryChange(
              parsedQuery.addAggregateColumn(columnName, 'MIN', aliasFactory(`min_${columnName}`)),
              true,
            );
          }}
        />
      </MenuItem>
    );
  }

  render(): JSX.Element {
    return (
      <>
        {this.renderFilterMenu()}
        {this.renderRemoveFilter()}
        {this.renderGroupByMenu()}
        {this.renderAggregateMenu()}
      </>
    );
  }
}
