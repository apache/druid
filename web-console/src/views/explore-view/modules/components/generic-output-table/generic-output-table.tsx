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

import { Button, Icon, Intent, Menu, MenuItem } from '@blueprintjs/core';
import type { IconName } from '@blueprintjs/icons';
import { IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import type { Column, QueryResult, SqlExpression } from '@druid-toolkit/query';
import { SqlColumn, SqlLiteral, trimString } from '@druid-toolkit/query';
import classNames from 'classnames';
import type { JSX } from 'react';
import React, { useEffect, useState } from 'react';
import type { Column as TableColumn } from 'react-table';
import ReactTable from 'react-table';

import { BracedText, Deferred, TableCell } from '../../../../../components';
import { possibleDruidFormatForValues, TIME_COLUMN } from '../../../../../druid-models';
import { SMALL_TABLE_PAGE_SIZE, SMALL_TABLE_PAGE_SIZE_OPTIONS } from '../../../../../react-table';
import type { Pagination, QueryAction } from '../../../../../utils';
import {
  columnToIcon,
  columnToWidth,
  copyAndAlert,
  formatNumber,
  getNumericColumnBraces,
  prettyPrintSql,
  stringifyValue,
  timeFormatToSql,
} from '../../../../../utils';

import './generic-output-table.scss';

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

function columnNester(columns: TableColumn[], groupHints: string[] | undefined): TableColumn[] {
  if (!groupHints) return columns;

  const ret: TableColumn[] = [];
  let currentGroupHint: string | null = null;
  let currentColumnGroup: TableColumn | null = null;
  for (let i = 0; i < columns.length; i++) {
    const column = columns[i];
    const groupHint = groupHints[i];
    if (groupHint) {
      if (currentGroupHint === groupHint) {
        currentColumnGroup!.columns!.push(column);
      } else {
        currentGroupHint = groupHint;
        ret.push(
          (currentColumnGroup = {
            Header: <div className="group-cell">{currentGroupHint}</div>,
            columns: [column],
          }),
        );
      }
    } else {
      ret.push(column);
      currentGroupHint = null;
      currentColumnGroup = null;
    }
  }

  return ret;
}

export interface GenericOutputTableProps {
  queryResult: QueryResult;
  onQueryAction(action: QueryAction): void;
  onOrderByChange?(columnIndex: number, desc: boolean): void;
  onExport?(): void;
  runeMode: boolean;
  showTypeIcons: boolean;
  initPageSize?: number;
  groupHints?: string[];
}

export const GenericOutputTable = React.memo(function GenericOutputTable(
  props: GenericOutputTableProps,
) {
  const {
    queryResult,
    onQueryAction,
    onOrderByChange,
    onExport,
    runeMode,
    showTypeIcons,
    initPageSize,
    groupHints,
  } = props;
  const parsedQuery = queryResult.sqlQuery;
  const [pagination, setPagination] = useState<Pagination>({
    page: 0,
    pageSize: initPageSize || 20,
  });

  // Reset page to 0 if number of results changes
  useEffect(() => {
    setPagination(pagination => {
      return pagination.page ? { ...pagination, page: 0 } : pagination;
    });
  }, [queryResult.rows.length]);

  function hasFilterOnHeader(header: string, headerIndex: number): boolean {
    if (!parsedQuery || !parsedQuery.isRealOutputColumnAtSelectIndex(headerIndex)) return false;

    return (
      parsedQuery.getEffectiveWhereExpression().containsColumnName(header) ||
      parsedQuery.getEffectiveHavingExpression().containsColumnName(header)
    );
  }

  function getHeaderMenu(column: Column, headerIndex: number) {
    const header = column.name;
    const ref = SqlColumn.create(header);
    const prettyRef = prettyPrintSql(ref);

    const menuItems: JSX.Element[] = [];
    if (parsedQuery) {
      const noStar = !parsedQuery.hasStarInSelect();
      const selectExpression = parsedQuery.getSelectExpressionForIndex(headerIndex);

      if (onOrderByChange) {
        const orderBy = parsedQuery.getOrderByForSelectIndex(headerIndex);

        if (orderBy) {
          const reverseOrderBy = orderBy.reverseDirection();
          const reverseOrderByDirection = reverseOrderBy.getEffectiveDirection();
          menuItems.push(
            <MenuItem
              key="order"
              icon={reverseOrderByDirection === 'ASC' ? IconNames.SORT_ASC : IconNames.SORT_DESC}
              text={`Order ${reverseOrderByDirection === 'ASC' ? 'ascending' : 'descending'}`}
              onClick={() => {
                onOrderByChange(headerIndex, reverseOrderByDirection !== 'ASC');
              }}
            />,
          );
        } else {
          menuItems.push(
            <MenuItem
              key="order_desc"
              icon={IconNames.SORT_DESC}
              text="Order descending"
              onClick={() => {
                onOrderByChange(headerIndex, true);
              }}
            />,
            <MenuItem
              key="order_asc"
              icon={IconNames.SORT_ASC}
              text="Order ascending"
              onClick={() => {
                onOrderByChange(headerIndex, false);
              }}
            />,
          );
        }
      }

      if (parsedQuery.isRealOutputColumnAtSelectIndex(headerIndex)) {
        const whereExpression = parsedQuery.getWhereExpression();
        if (whereExpression && whereExpression.containsColumnName(header)) {
          menuItems.push(
            <MenuItem
              key="remove_where"
              icon={IconNames.FILTER_REMOVE}
              text="Remove from WHERE clause"
              onClick={() => {
                onQueryAction(q =>
                  q.changeWhereExpression(whereExpression.removeColumnFromAnd(header)),
                );
              }}
            />,
          );
        }

        const havingExpression = parsedQuery.getHavingExpression();
        if (havingExpression && havingExpression.containsColumnName(header)) {
          menuItems.push(
            <MenuItem
              key="remove_having"
              icon={IconNames.FILTER_REMOVE}
              text="Remove from HAVING clause"
              onClick={() => {
                onQueryAction(q =>
                  q.changeHavingExpression(havingExpression.removeColumnFromAnd(header)),
                );
              }}
            />,
          );
        }
      }

      if (noStar && selectExpression) {
        if (column.isTimeColumn()) {
          // ToDo: clean
        } else if (column.sqlType === 'TIMESTAMP') {
          menuItems.push(
            <MenuItem
              key="declare_time"
              icon={IconNames.TIME}
              text="Use as the primary time column"
              onClick={() => {
                onQueryAction(q => q.changeSelect(headerIndex, selectExpression.as(TIME_COLUMN)));
              }}
            />,
          );
        } else {
          // Not a time column -------------------------------------------
          const values = queryResult.rows.map(row => row[headerIndex]);
          const possibleDruidFormat = possibleDruidFormatForValues(values);
          const formatSql = possibleDruidFormat ? timeFormatToSql(possibleDruidFormat) : undefined;

          if (formatSql) {
            const newSelectExpression = formatSql.fillPlaceholders([
              selectExpression.getUnderlyingExpression(),
            ]);

            menuItems.push(
              <MenuItem
                key="parse_time"
                icon={IconNames.TIME}
                text={`Time parse as '${possibleDruidFormat}' and use as the primary time column`}
                onClick={() => {
                  onQueryAction(q =>
                    q.changeSelect(headerIndex, newSelectExpression.as(TIME_COLUMN)),
                  );
                }}
              />,
            );
          }
        }
      }
    } else {
      menuItems.push(
        <MenuItem
          key="copy_ref"
          icon={IconNames.CLIPBOARD}
          text={`Copy: ${prettyRef}`}
          onClick={() => {
            copyAndAlert(String(ref), `${prettyRef}' copied to clipboard`);
          }}
        />,
      );

      if (!runeMode) {
        const orderByExpression = SqlColumn.create(header);
        const descOrderBy = orderByExpression.toOrderByExpression('DESC');
        const ascOrderBy = orderByExpression.toOrderByExpression('ASC');
        const descOrderByPretty = prettyPrintSql(descOrderBy);
        const ascOrderByPretty = prettyPrintSql(descOrderBy);

        menuItems.push(
          <MenuItem
            key="copy_desc"
            icon={IconNames.CLIPBOARD}
            text={`Copy: ${descOrderByPretty}`}
            onClick={() =>
              copyAndAlert(descOrderBy.toString(), `'${descOrderByPretty}' copied to clipboard`)
            }
          />,
          <MenuItem
            key="copy_asc"
            icon={IconNames.CLIPBOARD}
            text={`Copy: ${ascOrderByPretty}`}
            onClick={() =>
              copyAndAlert(ascOrderBy.toString(), `'${ascOrderByPretty}' copied to clipboard`)
            }
          />,
        );
      }
    }

    return <Menu>{menuItems}</Menu>;
  }

  function filterOnMenuItem(icon: IconName, clause: SqlExpression, having: boolean) {
    if (!parsedQuery) return;

    return (
      <MenuItem
        icon={icon}
        text={`${having ? 'Having' : 'Filter on'}: ${prettyPrintSql(clause)}`}
        onClick={() => {
          const columnName = clause.getUsedColumnNames()[0];
          onQueryAction(
            having
              ? q => q.removeFromHaving(columnName).addHaving(clause)
              : q => q.removeColumnFromWhere(columnName).addWhere(clause),
          );
        }}
      />
    );
  }

  function clipboardMenuItem(clause: SqlExpression) {
    const prettyLabel = prettyPrintSql(clause);
    return (
      <MenuItem
        icon={IconNames.CLIPBOARD}
        text={`Copy: ${prettyLabel}`}
        onClick={() => copyAndAlert(clause.toString(), `${prettyLabel} copied to clipboard`)}
      />
    );
  }

  function getCellMenu(column: Column, headerIndex: number, value: unknown) {
    const showFullValueMenuItem = (
      <MenuItem
        icon={IconNames.EYE_OPEN}
        text="Show full value"
        onClick={() => {
          // ToDo: clean up
        }}
      />
    );

    const val = sqlLiteralForColumnValue(column, value);

    if (parsedQuery) {
      let ex: SqlExpression | undefined;
      let having = false;
      if (parsedQuery.hasStarInSelect()) {
        ex = SqlColumn.create(column.name);
      } else {
        const selectValue = parsedQuery.getSelectExpressionForIndex(headerIndex);
        if (selectValue) {
          const outputName = selectValue.getOutputName();
          having = parsedQuery.isAggregateSelectIndex(headerIndex);
          if (having && outputName) {
            ex = SqlColumn.create(outputName);
          } else {
            ex = selectValue.getUnderlyingExpression();
          }
        }
      }

      const jsonColumn = column.nativeType === 'COMPLEX<json>';
      return (
        <Menu>
          {ex && val && !jsonColumn && (
            <>
              {filterOnMenuItem(IconNames.FILTER, ex.equal(val), having)}
              {filterOnMenuItem(IconNames.FILTER, ex.unequal(val), having)}
              {isComparable(value) && (
                <>
                  {filterOnMenuItem(IconNames.FILTER, ex.greaterThanOrEqual(val), having)}
                  {filterOnMenuItem(IconNames.FILTER, ex.lessThanOrEqual(val), having)}
                </>
              )}
            </>
          )}
          {showFullValueMenuItem}
        </Menu>
      );
    } else {
      const ref = SqlColumn.create(column.name);
      const stringValue = stringifyValue(value);
      const trimmedValue = trimString(stringValue, 50);
      return (
        <Menu>
          <MenuItem
            icon={IconNames.CLIPBOARD}
            text={`Copy: ${trimmedValue}`}
            onClick={() => copyAndAlert(stringValue, `${trimmedValue} copied to clipboard`)}
          />
          {!runeMode && val && (
            <>
              {clipboardMenuItem(ref.equal(val))}
              {clipboardMenuItem(ref.unequal(val))}
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

    if (parsedQuery.isAggregateOutputColumn(header)) {
      className.push('aggregate-header');
    }

    return className.join(' ');
  }

  const outerLimit = queryResult.getSqlOuterLimit();
  const hasMoreResults = queryResult.rows.length === outerLimit;
  const finalPage =
    hasMoreResults && Math.floor(queryResult.rows.length / pagination.pageSize) === pagination.page; // on the last page

  const numericColumnBraces = getNumericColumnBraces(queryResult, pagination);
  return (
    <div className={classNames('generic-output-table', { 'more-results': hasMoreResults })}>
      {finalPage ? (
        <div className="dead-end">
          <p>This is the end of the inline results but there are more results in this query.</p>
          {onExport && (
            <>
              <p>If you want to see the full list of results you should export them.</p>
              <Button
                icon={IconNames.DOWNLOAD}
                text="Export results"
                intent={Intent.PRIMARY}
                fill
                onClick={onExport}
              />
            </>
          )}
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
          columns={columnNester(
            queryResult.header.map((column, i) => {
              const h = column.name;
              const icon = showTypeIcons ? columnToIcon(column) : undefined;

              return {
                Header() {
                  return (
                    <Popover2 content={<Deferred content={() => getHeaderMenu(column, i)} />}>
                      <div className="clickable-cell">
                        <div className="output-name">
                          {icon && <Icon className="type-icon" icon={icon} size={12} />}
                          {h}
                          {hasFilterOnHeader(h, i) && <Icon icon={IconNames.FILTER} size={14} />}
                        </div>
                      </div>
                    </Popover2>
                  );
                },
                headerClassName: getHeaderClassName(h),
                accessor: String(i),
                Cell(row) {
                  const value = row.value;
                  return (
                    <div>
                      <Popover2
                        content={<Deferred content={() => getCellMenu(column, i, value)} />}
                      >
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
                className:
                  parsedQuery && parsedQuery.isAggregateOutputColumn(h)
                    ? 'aggregate-column'
                    : undefined,
              };
            }),
            groupHints,
          )}
        />
      )}
    </div>
  );
});
