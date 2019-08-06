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

import { Intent, Popover } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import copy = require('copy-to-clipboard');
import { HeaderRows } from 'druid-query-toolkit';
import {
  basicIdentifierEscape,
  basicLiteralEscape,
} from 'druid-query-toolkit/build/ast/sql-query/helpers';
import React from 'react';
import ReactTable from 'react-table';

import { AppToaster } from '../../../singletons/toaster';
import { BasicAction, basicActionsToMenu } from '../../../utils/basic-action';

import './query-output.scss';

export interface QueryOutputProps {
  aggregateColumns?: string[];
  disabled: boolean;
  loading: boolean;
  handleSQLAction: (row: string, header: string, action: string, direction?: boolean) => void;
  sorted?: { id: string; desc: boolean }[];
  result?: HeaderRows;
  error?: string;
}

export class QueryOutput extends React.PureComponent<QueryOutputProps> {
  render(): JSX.Element {
    const { result, loading, error } = this.props;

    return (
      <div className="query-output">
        <ReactTable
          data={result ? result.rows : []}
          loading={loading}
          noDataText={!loading && result && !result.rows.length ? 'No results' : error || ''}
          sortable={false}
          columns={(result ? result.header : []).map((h: any, i) => {
            return {
              Header: () => {
                return (
                  <Popover className={'clickable-cell'} content={this.getHeaderActions(h)}>
                    <div>{h}</div>
                  </Popover>
                );
              },
              headerClassName: this.getHeaderClassName(h),
              accessor: String(i),
              Cell: row => {
                const value = row.value;
                const popover = (
                  <div>
                    <Popover content={this.getRowActions(value, h)}>
                      <div>{value}</div>
                    </Popover>
                  </div>
                );
                if (value) {
                  return popover;
                }
                return value;
              },
              className: this.props.aggregateColumns
                ? this.props.aggregateColumns.indexOf(h) > -1
                  ? 'aggregate-column'
                  : undefined
                : undefined,
            };
          })}
        />
      </div>
    );
  }
  getHeaderActions(h: string) {
    const { disabled, handleSQLAction } = this.props;
    let actionsMenu;
    if (disabled) {
      actionsMenu = basicActionsToMenu([
        {
          title: `Copy ${basicIdentifierEscape(h)}`,
          onAction: () => {
            copy(basicIdentifierEscape(h), { format: 'text/plain' });
            AppToaster.show({
              message: `${basicIdentifierEscape(h)}' copied to clipboard`,
              intent: Intent.SUCCESS,
            });
          },
        },
        {
          title: `Copy 'ORDER BY ${basicIdentifierEscape(h)} ASC'`,
          onAction: () => {
            copy(`ORDER BY ${basicIdentifierEscape(h)} ASC`, { format: 'text/plain' });
            AppToaster.show({
              message: `ORDER BY ${basicIdentifierEscape(h)} ASC' copied to clipboard`,
              intent: Intent.SUCCESS,
            });
          },
        },
        {
          title: `Copy 'ORDER BY ${basicIdentifierEscape(h)} DESC'`,
          onAction: () => {
            copy(`ORDER BY ${basicIdentifierEscape(h)} DESC`, { format: 'text/plain' });
            AppToaster.show({
              message: `ORDER BY ${basicIdentifierEscape(h)} DESC' copied to clipboard`,
              intent: Intent.SUCCESS,
            });
          },
        },
      ]);
    } else {
      const { sorted } = this.props;
      const basicActions: BasicAction[] = [];
      if (sorted) {
        sorted.map(sorted => {
          if (sorted.id === h) {
            basicActions.push({
              icon: sorted.desc ? IconNames.SORT_ASC : IconNames.SORT_DESC,
              title: `Order by: ${h} ${sorted.desc ? 'ASC' : 'DESC'}`,
              onAction: () => handleSQLAction('', h, 'order by'),
            });
          }
        });
      }
      if (!basicActions.length) {
        basicActions.push(
          {
            icon: IconNames.SORT_ASC,
            title: `Order by: ${h} ASC`,
            onAction: () => handleSQLAction('', h, 'order by'),
          },
          {
            icon: IconNames.SORT_DESC,
            title: `Order by: ${h} DESC`,
            onAction: () => handleSQLAction('', h, 'order by'),
          },
        );
      }
      basicActions.push({
        icon: IconNames.CROSS,
        title: `Remove: ${h}`,
        onAction: () => handleSQLAction('', h, 'exclude column'),
      });
      actionsMenu = basicActionsToMenu(basicActions);
    }
    return actionsMenu ? actionsMenu : undefined;
  }

  getRowActions(row: string, header: string) {
    const { disabled, handleSQLAction } = this.props;
    let actionsMenu;
    if (disabled) {
      actionsMenu = basicActionsToMenu([
        {
          icon: IconNames.DUPLICATE,
          title: `Copy '${row}'`,
          onAction: () => {
            copy(row, { format: 'text/plain' });
            AppToaster.show({
              message: `${row} copied to clipboard`,
              intent: Intent.SUCCESS,
            });
          },
        },
        {
          icon: IconNames.DUPLICATE,
          title: `Copy 'WHERE ${basicIdentifierEscape(header)} = ${basicLiteralEscape(row)}'`,
          onAction: () => {
            copy(`WHERE  ${basicIdentifierEscape(header)} = ${basicLiteralEscape(row)}`, {
              format: 'text/plain',
            });
            AppToaster.show({
              message: `WHERE ${basicIdentifierEscape(header)} = ${basicLiteralEscape(
                row,
              )} copied to clipboard`,
              intent: Intent.SUCCESS,
            });
          },
        },
        {
          icon: IconNames.DUPLICATE,
          title: `Copy 'WHERE ${basicIdentifierEscape(header)} != ${basicLiteralEscape(row)}'`,
          onAction: () => {
            copy(`WHERE  ${basicIdentifierEscape(header)} != ${basicLiteralEscape(row)}`, {
              format: 'text/plain',
            });
            AppToaster.show({
              message: `WHERE ${basicIdentifierEscape(header)} != ${basicLiteralEscape(
                row,
              )} copied to clipboard`,
              intent: Intent.SUCCESS,
            });
          },
        },
      ]);
    } else {
      actionsMenu = basicActionsToMenu([
        {
          icon: IconNames.FILTER_KEEP,
          title: `Filter by: ${header} = ${row}`,
          onAction: () => handleSQLAction(row, header, 'exclude'),
        },
        {
          icon: IconNames.FILTER_REMOVE,
          title: `Filter by: ${header} != ${row}`,
          onAction: () => handleSQLAction(row, header, 'filter'),
        },
      ]);
    }
    return actionsMenu ? actionsMenu : undefined;
  }

  getHeaderClassName(h: string) {
    const { sorted } = this.props;
    const className = [];
    className.push(
      sorted
        ? sorted.map(sorted => {
            if (sorted.id === h) {
              return sorted.desc ? '-sort-desc' : '-sort-asc';
            }
            return '';
          })[0]
        : undefined,
    );
    if (this.props.aggregateColumns) {
      if (this.props.aggregateColumns.indexOf(h) > -1) {
        className.push('aggregate-header');
      }
    }

    return className.join(' ');
  }
}
