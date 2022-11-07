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

import { Icon } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import classNames from 'classnames';
import { Column, QueryResult, SqlAlias, SqlQuery, SqlStar } from 'druid-query-toolkit';
import React, { useState } from 'react';
import ReactTable from 'react-table';

import { BracedText, Deferred, TableCell } from '../../../../components';
import { CellFilterMenu } from '../../../../components/cell-filter-menu/cell-filter-menu';
import { ShowValueDialog } from '../../../../dialogs/show-value-dialog/show-value-dialog';
import {
  columnToIcon,
  columnToWidth,
  filterMap,
  getNumericColumnBraces,
  QueryAction,
} from '../../../../utils';

import './preview-table.scss';

function isDate(v: any): v is Date {
  return Boolean(v && typeof v.toISOString === 'function');
}

function getExpressionIfAlias(query: SqlQuery, selectIndex: number): string {
  const ex = query.getSelectExpressionForIndex(selectIndex);

  if (query.isRealOutputColumnAtSelectIndex(selectIndex)) {
    if (ex instanceof SqlAlias) {
      return String(ex.expression.prettify({ keywordCasing: 'preserve' }));
    } else {
      return '';
    }
  } else if (ex instanceof SqlStar) {
    return '';
  } else {
    return ex ? String(ex.prettify({ keywordCasing: 'preserve' })) : '';
  }
}

export interface PreviewTableProps {
  queryResult: QueryResult;
  onQueryAction(action: QueryAction): void;
  columnFilter?(columnName: string): boolean;
  selectedColumnIndex: number;
  onEditColumn(index: number): void;
}

export const PreviewTable = React.memo(function PreviewTable(props: PreviewTableProps) {
  const { queryResult, onQueryAction, columnFilter, selectedColumnIndex, onEditColumn } = props;
  const [showValue, setShowValue] = useState<string>();

  const parsedQuery: SqlQuery = queryResult.sqlQuery!;
  if (!parsedQuery) return null;

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
        onQueryAction={onQueryAction}
        onShowFullValue={setShowValue}
      />
    );
  }

  const numericColumnBraces = getNumericColumnBraces(queryResult);
  return (
    <div className="preview-table">
      <ReactTable
        className="-striped -highlight"
        data={queryResult.rows as any[][]}
        noDataText={queryResult.rows.length ? '' : 'Preview returned no data'}
        defaultPageSize={25}
        showPagination={false}
        sortable={false}
        columns={filterMap(queryResult.header, (column, i) => {
          const h = column.name;
          if (columnFilter && !columnFilter(h)) return;

          const icon = columnToIcon(column);
          const selected = selectedColumnIndex === i;

          const columnClassName = parsedQuery.isAggregateSelectIndex(i)
            ? classNames('metric', `column${i}`, {
                selected,
                'first-metric': i > 0 && !parsedQuery.isAggregateSelectIndex(i - 1),
              })
            : classNames(
                column.isTimeColumn() ? 'timestamp' : 'dimension',
                `column${i}`,
                column.sqlType?.toLowerCase(),
                { selected },
              );

          return {
            Header() {
              return (
                <div className="header-wrapper" onClick={() => onEditColumn(i)}>
                  <div className="output-name">
                    {icon && <Icon className="type-icon" icon={icon} size={12} />}
                    {h}
                    {hasFilterOnHeader(h, i) && (
                      <Icon className="filter-icon" icon={IconNames.FILTER} size={14} />
                    )}
                  </div>
                  <div className="formula">{getExpressionIfAlias(parsedQuery, i)}</div>
                </div>
              );
            },
            headerClassName: columnClassName,
            className: columnClassName,
            width: columnToWidth(column),
            accessor: String(i),
            Cell(row) {
              const value = row.value;
              return (
                <div>
                  <Popover2 content={<Deferred content={() => getCellMenu(column, i, value)} />}>
                    {numericColumnBraces[i] ? (
                      <BracedText
                        className="table-padding"
                        text={isDate(value) ? value.toISOString() : String(value)}
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
          };
        })}
      />
      {showValue && <ShowValueDialog onClose={() => setShowValue(undefined)} str={showValue} />}
    </div>
  );
});
