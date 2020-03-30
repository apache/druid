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
import { basicIdentifierEscape, basicLiteralEscape } from 'druid-query-toolkit/build/sql/helpers';
import React, { useState } from 'react';
import ReactTable from 'react-table';

import { TableCell } from '../../../components';
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

export const QueryOutput = React.memo(function QueryOutput(props: QueryOutputProps) {
  const { queryResult, parsedQuery, loading, error } = props;
  const [showValue, setShowValue] = useState();

  function getHeaderMenu(header: string) {
    const { parsedQuery, onQueryChange, runeMode } = props;

    if (parsedQuery) {
      const sorted = parsedQuery.getSorted();

      const basicActions: BasicAction[] = [];
      if (sorted) {
        sorted.map(sorted => {
          if (sorted.id === header) {
            basicActions.push({
              icon: sorted.desc ? IconNames.SORT_ASC : IconNames.SORT_DESC,
              title: `Order by: ${trimValue(header)} ${sorted.desc ? 'ASC' : 'DESC'}`,
              onAction: () => {
                onQueryChange(parsedQuery.orderBy(header, sorted.desc ? 'ASC' : 'DESC'), true);
              },
            });
          }
        });
      }
      if (!basicActions.length) {
        basicActions.push(
          {
            icon: IconNames.SORT_DESC,
            title: `Order by: ${trimValue(header)} DESC`,
            onAction: () => {
              onQueryChange(parsedQuery.orderBy(header, 'DESC'), true);
            },
          },
          {
            icon: IconNames.SORT_ASC,
            title: `Order by: ${trimValue(header)} ASC`,
            onAction: () => {
              onQueryChange(parsedQuery.orderBy(header, 'ASC'), true);
            },
          },
        );
      }
      basicActions.push({
        icon: IconNames.CROSS,
        title: `Remove: ${trimValue(header)}`,
        onAction: () => {
          onQueryChange(parsedQuery.remove(header), true);
        },
      });

      return basicActionsToMenu(basicActions);
    } else {
      return (
        <Menu>
          <MenuItem
            icon={IconNames.CLIPBOARD}
            text={`Copy: ${trimValue(header)}`}
            onClick={() => {
              copyAndAlert(header, `${header}' copied to clipboard`);
            }}
          />
          {!runeMode && (
            <>
              <MenuItem
                icon={IconNames.CLIPBOARD}
                text={`Copy: ORDER BY ${basicIdentifierEscape(header)} ASC`}
                onClick={() =>
                  copyAndAlert(
                    `ORDER BY ${basicIdentifierEscape(header)} ASC`,
                    `ORDER BY ${basicIdentifierEscape(header)} ASC' copied to clipboard`,
                  )
                }
              />
              <MenuItem
                icon={IconNames.CLIPBOARD}
                text={`Copy: 'ORDER BY ${basicIdentifierEscape(header)} DESC'`}
                onClick={() =>
                  copyAndAlert(
                    `ORDER BY ${basicIdentifierEscape(header)} DESC`,
                    `ORDER BY ${basicIdentifierEscape(header)} DESC' copied to clipboard`,
                  )
                }
              />
            </>
          )}
        </Menu>
      );
    }
  }

  function getCellMenu(header: string, value: any) {
    const { parsedQuery, onQueryChange, runeMode } = props;

    const showFullValueMenuItem =
      typeof value === 'string' ? (
        <MenuItem
          icon={IconNames.EYE_OPEN}
          text={`Show full value`}
          onClick={() => {
            setShowValue(value);
          }}
        />
      ) : (
        undefined
      );

    if (parsedQuery) {
      return (
        <Menu>
          <MenuItem
            icon={IconNames.FILTER_KEEP}
            text={`Filter by: ${trimValue(header)} = ${trimValue(value)}`}
            onClick={() => {
              onQueryChange(parsedQuery.addWhereFilter(header, '=', value), true);
            }}
          />
          <MenuItem
            icon={IconNames.FILTER_REMOVE}
            text={`Filter by: ${trimValue(header)} != ${trimValue(value)}`}
            onClick={() => {
              onQueryChange(parsedQuery.addWhereFilter(header, '!=', value), true);
            }}
          />
          {!isNaN(Number(value)) && (
            <>
              <MenuItem
                icon={IconNames.FILTER_KEEP}
                text={`Filter by: ${trimValue(header)} >= ${trimValue(value)}`}
                onClick={() => {
                  onQueryChange(parsedQuery.addWhereFilter(header, '>=', value), true);
                }}
              />
              <MenuItem
                icon={IconNames.FILTER_KEEP}
                text={`Filter by: ${trimValue(header)} <= ${trimValue(value)}`}
                onClick={() => {
                  onQueryChange(parsedQuery.addWhereFilter(header, '<=', value), true);
                }}
              />
            </>
          )}
          {showFullValueMenuItem}
        </Menu>
      );
    } else {
      return (
        <Menu>
          <MenuItem
            icon={IconNames.CLIPBOARD}
            text={`Copy: ${trimValue(value)}`}
            onClick={() => copyAndAlert(value, `${value} copied to clipboard`)}
          />
          {!runeMode && (
            <>
              <MenuItem
                icon={IconNames.CLIPBOARD}
                text={`Copy: ${basicIdentifierEscape(header)} = ${basicLiteralEscape(value)}`}
                onClick={() =>
                  copyAndAlert(
                    `${basicIdentifierEscape(header)} = ${basicLiteralEscape(value)}`,
                    `${basicIdentifierEscape(header)} = ${basicLiteralEscape(
                      value,
                    )} copied to clipboard`,
                  )
                }
              />
              <MenuItem
                icon={IconNames.CLIPBOARD}
                text={`Copy: ${basicIdentifierEscape(header)} != ${basicLiteralEscape(value)}`}
                onClick={() =>
                  copyAndAlert(
                    `${basicIdentifierEscape(header)} != ${basicLiteralEscape(value)}`,
                    `${basicIdentifierEscape(header)} != ${basicLiteralEscape(
                      value,
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
  }

  function getHeaderClassName(header: string) {
    const { parsedQuery } = props;
    if (!parsedQuery) return;

    const className = [];
    const sorted = parsedQuery.getSorted();
    const aggregateColumns = parsedQuery.getAggregateColumns();

    if (sorted) {
      const sortedColumnNames = sorted.map(column => column.id);
      if (sortedColumnNames.includes(header)) {
        className.push(sorted[sortedColumnNames.indexOf(header)].desc ? '-sort-desc' : '-sort-asc');
      }
    }

    if (aggregateColumns && aggregateColumns.includes(header)) {
      className.push('aggregate-header');
    }

    return className.join(' ');
  }

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
                <Popover className={'clickable-cell'} content={getHeaderMenu(h)}>
                  <div>{h}</div>
                </Popover>
              );
            },
            headerClassName: getHeaderClassName(h),
            accessor: String(i),
            Cell: row => {
              const value = row.value;
              return (
                <div>
                  <Popover content={getCellMenu(h, value)}>
                    <TableCell value={value} unlimited />
                  </Popover>
                </div>
              );
            },
            className:
              aggregateColumns && aggregateColumns.includes(h) ? 'aggregate-column' : undefined,
          };
        })}
      />
      {showValue && <ShowValueDialog onClose={() => setShowValue(undefined)} str={showValue} />}
    </div>
  );
});
