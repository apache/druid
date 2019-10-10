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

import { Menu, MenuItem, Popover } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { HeaderRows, SqlQuery } from 'druid-query-toolkit';
import {
  basicIdentifierEscape,
  basicLiteralEscape,
} from 'druid-query-toolkit/build/ast/sql-query/helpers';
import React from 'react';
import ReactTable from 'react-table';

import { ShowValueDialog } from '../../../dialogs/show-value-dialog/show-value-dialog';
import { copyAndAlert } from '../../../utils';
import { BasicAction, basicActionsToMenu } from '../../../utils/basic-action';

import './query-output.scss';

function trimValue(str: any): string {
  str = String(str);
  if (str.length < 102) return str;
  return str.substr(0, 100) + '...';
}

export interface QueryOutputProps {
  loading: boolean;
  queryResult?: HeaderRows;
  parsedQuery?: SqlQuery;
  onQueryChange: (query: SqlQuery, run?: boolean) => void;
  error?: string;
  runeMode: boolean;
}

export interface QueryOutputState {
  showValue?: string;
}

export class QueryOutput extends React.PureComponent<QueryOutputProps, QueryOutputState> {
  constructor(props: QueryOutputProps) {
    super(props);
    this.state = {};
  }

  renderShowValueDialog(): JSX.Element | undefined {
    const { showValue } = this.state;
    if (!showValue) return;

    return (
      <ShowValueDialog onClose={() => this.setState({ showValue: undefined })} str={showValue} />
    );
  }

  render(): JSX.Element {
    const { queryResult, parsedQuery, loading, error } = this.props;

    let aggregateColumns: string[] | undefined;
    if (parsedQuery) {
      aggregateColumns = parsedQuery.getAggregateColumns();
    }

    return (
      <div className="query-output">
        <ReactTable
          data={queryResult ? queryResult.rows : []}
          loading={loading}
          noDataText={
            !loading && queryResult && !queryResult.rows.length
              ? 'Query returned no data'
              : error || ''
          }
          sortable={false}
          columns={(queryResult ? queryResult.header : []).map((h: any, i) => {
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
                if (!value) return value == null ? null : value;
                return (
                  <div>
                    <Popover content={this.getRowActions(value, h)}>
                      <div>{value}</div>
                    </Popover>
                  </div>
                );
              },
              className:
                aggregateColumns && aggregateColumns.includes(h) ? 'aggregate-column' : undefined,
            };
          })}
        />
        {this.renderShowValueDialog()}
      </div>
    );
  }

  getHeaderActions(h: string) {
    const { parsedQuery, onQueryChange, runeMode } = this.props;

    let actionsMenu;
    if (parsedQuery) {
      const sorted = parsedQuery.getSorted();

      const basicActions: BasicAction[] = [];
      if (sorted) {
        sorted.map(sorted => {
          if (sorted.id === h) {
            basicActions.push({
              icon: sorted.desc ? IconNames.SORT_ASC : IconNames.SORT_DESC,
              title: `Order by: ${trimValue(h)} ${sorted.desc ? 'ASC' : 'DESC'}`,
              onAction: () => {
                onQueryChange(parsedQuery.orderBy(h, sorted.desc ? 'ASC' : 'DESC'), true);
              },
            });
          }
        });
      }
      if (!basicActions.length) {
        basicActions.push(
          {
            icon: IconNames.SORT_DESC,
            title: `Order by: ${trimValue(h)} DESC`,
            onAction: () => {
              onQueryChange(parsedQuery.orderBy(h, 'DESC'), true);
            },
          },
          {
            icon: IconNames.SORT_ASC,
            title: `Order by: ${trimValue(h)} ASC`,
            onAction: () => {
              onQueryChange(parsedQuery.orderBy(h, 'ASC'), true);
            },
          },
        );
      }
      basicActions.push({
        icon: IconNames.CROSS,
        title: `Remove: ${trimValue(h)}`,
        onAction: () => {
          onQueryChange(parsedQuery.excludeColumn(h), true);
        },
      });
      actionsMenu = basicActionsToMenu(basicActions);
    } else {
      actionsMenu = (
        <Menu>
          <MenuItem
            icon={IconNames.CLIPBOARD}
            text={`Copy: ${trimValue(h)}`}
            onClick={() => {
              copyAndAlert(h, `${h}' copied to clipboard`);
            }}
          />
          {!runeMode && (
            <>
              <MenuItem
                icon={IconNames.CLIPBOARD}
                text={`Copy: ORDER BY ${basicIdentifierEscape(h)} ASC`}
                onClick={() =>
                  copyAndAlert(
                    `ORDER BY ${basicIdentifierEscape(h)} ASC`,
                    `ORDER BY ${basicIdentifierEscape(h)} ASC' copied to clipboard`,
                  )
                }
              />
              <MenuItem
                icon={IconNames.CLIPBOARD}
                text={`Copy: 'ORDER BY ${basicIdentifierEscape(h)} DESC'`}
                onClick={() =>
                  copyAndAlert(
                    `ORDER BY ${basicIdentifierEscape(h)} DESC`,
                    `ORDER BY ${basicIdentifierEscape(h)} DESC' copied to clipboard`,
                  )
                }
              />
            </>
          )}
        </Menu>
      );
    }
    return actionsMenu ? actionsMenu : undefined;
  }

  getRowActions(row: any, header: string) {
    const { parsedQuery, onQueryChange, runeMode } = this.props;

    const showFullValueMenuItem =
      typeof row === 'string' ? (
        <MenuItem
          icon={IconNames.EYE_OPEN}
          text={`Show full value`}
          onClick={() => {
            this.setState({ showValue: row });
          }}
        />
      ) : (
        undefined
      );

    let actionsMenu;
    if (parsedQuery) {
      actionsMenu = (
        <Menu>
          <MenuItem
            icon={IconNames.FILTER_KEEP}
            text={`Filter by: ${trimValue(header)} = ${trimValue(row)}`}
            onClick={() => {
              onQueryChange(parsedQuery.filterRow(header, row, '='), true);
            }}
          />
          <MenuItem
            icon={IconNames.FILTER_REMOVE}
            text={`Filter by: ${trimValue(header)} != ${trimValue(row)}`}
            onClick={() => {
              onQueryChange(parsedQuery.filterRow(header, row, '!='), true);
            }}
          />
          {!isNaN(Number(row)) && (
            <>
              <MenuItem
                icon={IconNames.FILTER_KEEP}
                text={`Filter by: ${trimValue(header)} >= ${trimValue(row)}`}
                onClick={() => {
                  onQueryChange(parsedQuery.filterRow(header, row, '>='), true);
                }}
              />
              <MenuItem
                icon={IconNames.FILTER_KEEP}
                text={`Filter by: ${trimValue(header)} <= ${trimValue(row)}`}
                onClick={() => {
                  onQueryChange(parsedQuery.filterRow(header, row, '<='), true);
                }}
              />
            </>
          )}
          {showFullValueMenuItem}
        </Menu>
      );
    } else {
      actionsMenu = (
        <Menu>
          <MenuItem
            icon={IconNames.CLIPBOARD}
            text={`Copy: ${trimValue(row)}`}
            onClick={() => copyAndAlert(row, `${row} copied to clipboard`)}
          />
          {!runeMode && (
            <>
              <MenuItem
                icon={IconNames.CLIPBOARD}
                text={`Copy: ${basicIdentifierEscape(header)} = ${basicLiteralEscape(row)}`}
                onClick={() =>
                  copyAndAlert(
                    `${basicIdentifierEscape(header)} = ${basicLiteralEscape(row)}`,
                    `${basicIdentifierEscape(header)} = ${basicLiteralEscape(
                      row,
                    )} copied to clipboard`,
                  )
                }
              />
              <MenuItem
                icon={IconNames.CLIPBOARD}
                text={`Copy: ${basicIdentifierEscape(header)} != ${basicLiteralEscape(row)}`}
                onClick={() =>
                  copyAndAlert(
                    `${basicIdentifierEscape(header)} != ${basicLiteralEscape(row)}`,
                    `${basicIdentifierEscape(header)} != ${basicLiteralEscape(
                      row,
                    )} copied to clipboard`,
                  )
                }
              />
            </>
          )}
          {showFullValueMenuItem}
        </Menu>
      );
    }
    return actionsMenu;
  }

  getHeaderClassName(h: string) {
    const { parsedQuery } = this.props;

    const className = [];
    if (parsedQuery) {
      const sorted = parsedQuery.getSorted();
      if (sorted) {
        className.push(
          sorted.map(sorted => {
            if (sorted.id === h) {
              return sorted.desc ? '-sort-desc' : '-sort-asc';
            }
            return '';
          })[0],
        );
      }

      const aggregateColumns = parsedQuery.getAggregateColumns();
      if (aggregateColumns && aggregateColumns.includes(h)) {
        className.push('aggregate-header');
      }
    }

    return className.join(' ');
  }
}
