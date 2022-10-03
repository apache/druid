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

import { Button, Icon } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import classNames from 'classnames';
import { Column, QueryResult } from 'druid-query-toolkit';
import React, { useEffect, useState } from 'react';
import ReactTable from 'react-table';

import { ShowValueDialog } from '../../dialogs/show-value-dialog/show-value-dialog';
import { SMALL_TABLE_PAGE_SIZE, SMALL_TABLE_PAGE_SIZE_OPTIONS } from '../../react-table';
import {
  columnToIcon,
  columnToWidth,
  filterMap,
  formatNumber,
  getNumericColumnBraces,
  Pagination,
} from '../../utils';
import { BracedText } from '../braced-text/braced-text';
import { CellFilterMenu } from '../cell-filter-menu/cell-filter-menu';
import { Deferred } from '../deferred/deferred';
import { TableCell } from '../table-cell/table-cell';

import './record-table-pane.scss';

export interface RecordTablePaneProps {
  queryResult: QueryResult;
  initPageSize?: number;
}

export const RecordTablePane = React.memo(function RecordTablePane(props: RecordTablePaneProps) {
  const { queryResult, initPageSize } = props;
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

  function getCellMenu(column: Column, headerIndex: number, value: unknown) {
    return (
      <CellFilterMenu
        column={column}
        value={value}
        headerIndex={headerIndex}
        query={parsedQuery}
        onQueryAction={undefined}
        onShowFullValue={setShowValue}
      />
    );
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
