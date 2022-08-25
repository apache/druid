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

import { Button, Icon, Menu, MenuItem } from '@blueprintjs/core';
import { IconName, IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import classNames from 'classnames';
import {
  Column,
  QueryResult,
  SqlExpression,
  SqlLiteral,
  SqlRef,
  trimString,
} from 'druid-query-toolkit';
import React, { useEffect, useState } from 'react';
import ReactTable from 'react-table';

import { ShowValueDialog } from '../../dialogs/show-value-dialog/show-value-dialog';
import { SMALL_TABLE_PAGE_SIZE, SMALL_TABLE_PAGE_SIZE_OPTIONS } from '../../react-table';
import {
  columnToIcon,
  columnToWidth,
  copyAndAlert,
  filterMap,
  formatNumber,
  getNumericColumnBraces,
  Pagination,
  prettyPrintSql,
  stringifyValue,
} from '../../utils';
import { BracedText } from '../braced-text/braced-text';
import { Deferred } from '../deferred/deferred';
import { TableCell } from '../table-cell/table-cell';

import './record-table-pane.scss';

function sqlLiteralForColumnValue(column: Column, value: unknown): SqlLiteral | undefined {
  if (column.sqlType === 'TIMESTAMP') {
    const asDate = new Date(value as any);
    if (!isNaN(asDate.valueOf())) {
      return SqlLiteral.create(asDate);
    }
  }

  return SqlLiteral.maybe(value);
}

function isComparable(x: unknown): boolean {
  return x !== null && x !== '';
}

export interface RecordTablePaneProps {
  queryResult: QueryResult;
  initPageSize?: number;
  addFilter?(filter: string): void;
}

export const RecordTablePane = React.memo(function RecordTablePane(props: RecordTablePaneProps) {
  const { queryResult, initPageSize, addFilter } = props;
  const parsedQuery = queryResult.sqlQuery;
  const [pagination, setPagination] = useState<Pagination>({
    page: 0,
    pageSize: initPageSize || 20,
  });
  const [showValue, setShowValue] = useState<string>();

  // Reset page to 0 if number of results changes
  useEffect(() => {
    setPagination(pagination => {
      return pagination.page ? { ...pagination, page: 0 } : pagination;
    });
  }, [queryResult.rows.length]);

  function hasFilterOnHeader(header: string, headerIndex: number): boolean {
    if (!parsedQuery || !parsedQuery.isRealOutputColumnAtSelectIndex(headerIndex)) return false;

    return (
      parsedQuery.getEffectiveWhereExpression().containsColumn(header) ||
      parsedQuery.getEffectiveHavingExpression().containsColumn(header)
    );
  }

  function filterOnMenuItem(icon: IconName, clause: SqlExpression) {
    if (!parsedQuery || !addFilter) return;

    return (
      <MenuItem
        icon={icon}
        text={`Filter on: ${prettyPrintSql(clause)}`}
        onClick={() => {
          addFilter(clause.toString());
        }}
      />
    );
  }

  function actionMenuItem(clause: SqlExpression) {
    if (!addFilter) return;
    const prettyLabel = prettyPrintSql(clause);
    return (
      <MenuItem
        icon={IconNames.FILTER}
        text={`Filter: ${prettyLabel}`}
        onClick={() => addFilter(clause.toString())}
      />
    );
  }

  function getCellMenu(column: Column, headerIndex: number, value: unknown) {
    const showFullValueMenuItem = (
      <MenuItem
        icon={IconNames.EYE_OPEN}
        text="Show full value"
        onClick={() => {
          setShowValue(stringifyValue(value));
        }}
      />
    );

    const val = sqlLiteralForColumnValue(column, value);

    if (parsedQuery) {
      let ex: SqlExpression | undefined;
      if (parsedQuery.hasStarInSelect()) {
        ex = SqlRef.column(column.name);
      } else {
        const selectValue = parsedQuery.getSelectExpressionForIndex(headerIndex);
        if (selectValue) {
          ex = selectValue.getUnderlyingExpression();
        }
      }

      const jsonColumn = column.nativeType === 'COMPLEX<json>';
      return (
        <Menu>
          {ex && val && !jsonColumn && (
            <>
              {filterOnMenuItem(IconNames.FILTER, ex.equal(val))}
              {filterOnMenuItem(IconNames.FILTER, ex.unequal(val))}
              {isComparable(value) && (
                <>
                  {filterOnMenuItem(IconNames.FILTER, ex.greaterThanOrEqual(val))}
                  {filterOnMenuItem(IconNames.FILTER, ex.lessThanOrEqual(val))}
                </>
              )}
            </>
          )}
          {showFullValueMenuItem}
        </Menu>
      );
    } else {
      const ref = SqlRef.column(column.name);
      const stringValue = stringifyValue(value);
      const trimmedValue = trimString(stringValue, 50);
      return (
        <Menu>
          <MenuItem
            icon={IconNames.CLIPBOARD}
            text={`Copy: ${trimmedValue}`}
            onClick={() => copyAndAlert(stringValue, `${trimmedValue} copied to clipboard`)}
          />
          {val && (
            <>
              {actionMenuItem(ref.equal(val))}
              {actionMenuItem(ref.unequal(val))}
            </>
          )}
          {showFullValueMenuItem}
        </Menu>
      );
    }
  }

  function getHeaderClassName(header: string) {
    if (!parsedQuery) return;

    const className = [];

    const orderBy = parsedQuery.getOrderByForOutputColumn(header);
    if (orderBy) {
      className.push(orderBy.getEffectiveDirection() === 'DESC' ? '-sort-desc' : '-sort-asc');
    }

    return className.join(' ');
  }

  const outerLimit = queryResult.getSqlOuterLimit();
  const hasMoreResults = queryResult.rows.length === outerLimit;
  const finalPage =
    hasMoreResults && Math.floor(queryResult.rows.length / pagination.pageSize) === pagination.page; // on the last page

  const numericColumnBraces = getNumericColumnBraces(queryResult, pagination);
  return (
    <div className={classNames('record-table-pane', { 'more-results': hasMoreResults })}>
      {finalPage ? (
        <div className="dead-end">
          <p>This is the end of the inline results but there are more results in this query.</p>
          <Button
            icon={IconNames.ARROW_LEFT}
            text="Go to previous page"
            fill
            onClick={() => setPagination({ ...pagination, page: pagination.page - 1 })}
          />
        </div>
      ) : (
        <ReactTable
          className="-striped -highlight"
          data={queryResult.rows as any[][]}
          ofText={hasMoreResults ? '' : 'of'}
          noDataText={queryResult.rows.length ? '' : 'Query returned no data'}
          page={pagination.page}
          pageSize={pagination.pageSize}
          onPageChange={page => setPagination({ ...pagination, page })}
          onPageSizeChange={(pageSize, page) => setPagination({ page, pageSize })}
          sortable={false}
          defaultPageSize={SMALL_TABLE_PAGE_SIZE}
          pageSizeOptions={SMALL_TABLE_PAGE_SIZE_OPTIONS}
          showPagination={
            queryResult.rows.length > Math.min(SMALL_TABLE_PAGE_SIZE, pagination.pageSize)
          }
          columns={filterMap(queryResult.header, (column, i) => {
            const h = column.name;
            const icon = columnToIcon(column);

            return {
              Header() {
                return (
                  <div className="clickable-cell">
                    <div className="output-name">
                      {icon && <Icon className="type-icon" icon={icon} size={12} />}
                      {h}
                      {hasFilterOnHeader(h, i) && <Icon icon={IconNames.FILTER} size={14} />}
                    </div>
                  </div>
                );
              },
              headerClassName: getHeaderClassName(h),
              accessor: String(i),
              Cell(row) {
                const value = row.value;
                return (
                  <div>
                    <Popover2 content={<Deferred content={() => getCellMenu(column, i, value)} />}>
                      {numericColumnBraces[i] ? (
                        <BracedText
                          className="table-padding"
                          text={formatNumber(value)}
                          braces={numericColumnBraces[i]}
                          padFractionalPart
                        />
                      ) : (
                        <TableCell value={value} unlimited />
                      )}
                    </Popover2>
                  </div>
                );
              },
              width: columnToWidth(column),
            };
          })}
        />
      )}
      {showValue && <ShowValueDialog onClose={() => setShowValue(undefined)} str={showValue} />}
    </div>
  );
});
