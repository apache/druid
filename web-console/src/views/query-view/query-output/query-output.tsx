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

import { Icon, Menu, MenuItem } from '@blueprintjs/core';
import { IconName, IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import classNames from 'classnames';
import {
  QueryResult,
  SqlExpression,
  SqlLiteral,
  SqlQuery,
  SqlRef,
  trimString,
} from 'druid-query-toolkit';
import React, { useEffect, useState } from 'react';
import ReactTable from 'react-table';

import { BracedText, TableCell } from '../../../components';
import { ShowValueDialog } from '../../../dialogs/show-value-dialog/show-value-dialog';
import { copyAndAlert, deepSet, filterMap, prettyPrintSql, stringifyValue } from '../../../utils';
import { BasicAction, basicActionsToMenu } from '../../../utils/basic-action';

import { ColumnRenameInput } from './column-rename-input/column-rename-input';

import './query-output.scss';

function isComparable(x: unknown): boolean {
  return x !== null && x !== '' && !isNaN(Number(x));
}

interface Pagination {
  page: number;
  pageSize: number;
}

function changePage(pagination: Pagination, page: number): Pagination {
  return deepSet(pagination, 'page', page);
}

function getNumericColumnBraces(
  queryResult: QueryResult,
  pagination: Pagination,
): Record<number, string[]> {
  const numericColumnBraces: Record<number, string[]> = {};

  const index = pagination.page * pagination.pageSize;
  const rows = queryResult.rows.slice(index, index + pagination.pageSize);
  if (rows.length) {
    const numColumns = queryResult.header.length;
    for (let c = 0; c < numColumns; c++) {
      const brace = filterMap(rows, row =>
        typeof row[c] === 'number' ? String(row[c]) : undefined,
      );
      if (rows.length === brace.length) {
        numericColumnBraces[c] = brace;
      }
    }
  }

  return numericColumnBraces;
}

export interface QueryOutputProps {
  queryResult: QueryResult;
  onQueryChange: (query: SqlQuery, run?: boolean) => void;
  onLoadMore: () => void;
  runeMode: boolean;
}

export const QueryOutput = React.memo(function QueryOutput(props: QueryOutputProps) {
  const { queryResult, onQueryChange, onLoadMore, runeMode } = props;
  const parsedQuery = queryResult.sqlQuery;
  const [pagination, setPagination] = useState<Pagination>({ page: 0, pageSize: 20 });
  const [showValue, setShowValue] = useState<string>();
  const [renamingColumn, setRenamingColumn] = useState<number>(-1);

  // Reset page to 0 if number of results changes
  useEffect(() => {
    if (pagination.page) {
      setPagination(changePage(pagination, 0));
    }
  }, [queryResult.rows.length]);

  function hasFilterOnHeader(header: string, headerIndex: number): boolean {
    if (!parsedQuery || !parsedQuery.isRealOutputColumnAtSelectIndex(headerIndex)) return false;

    return (
      parsedQuery.getEffectiveWhereExpression().containsColumn(header) ||
      parsedQuery.getEffectiveHavingExpression().containsColumn(header)
    );
  }

  function getHeaderMenu(header: string, headerIndex: number) {
    const ref = SqlRef.column(header);
    const prettyRef = prettyPrintSql(ref);

    if (parsedQuery) {
      const orderByExpression = parsedQuery.isValidSelectIndex(headerIndex)
        ? SqlLiteral.index(headerIndex)
        : SqlRef.column(header);
      const descOrderBy = orderByExpression.toOrderByPart('DESC');
      const ascOrderBy = orderByExpression.toOrderByPart('ASC');
      const orderBy = parsedQuery.getOrderByForSelectIndex(headerIndex);

      const basicActions: BasicAction[] = [];
      if (orderBy) {
        const reverseOrderBy = orderBy.reverseDirection();
        const reverseOrderByDirection = reverseOrderBy.getEffectiveDirection();
        basicActions.push({
          icon: reverseOrderByDirection === 'ASC' ? IconNames.SORT_ASC : IconNames.SORT_DESC,
          title: `Order ${reverseOrderByDirection === 'ASC' ? 'ascending' : 'descending'}`,
          onAction: () => {
            onQueryChange(parsedQuery.changeOrderByExpressions([reverseOrderBy]), true);
          },
        });
      } else {
        basicActions.push(
          {
            icon: IconNames.SORT_DESC,
            title: `Order descending`,
            onAction: () => {
              onQueryChange(parsedQuery.changeOrderByExpressions([descOrderBy]), true);
            },
          },
          {
            icon: IconNames.SORT_ASC,
            title: `Order ascending`,
            onAction: () => {
              onQueryChange(parsedQuery.changeOrderByExpressions([ascOrderBy]), true);
            },
          },
        );
      }

      if (parsedQuery.isRealOutputColumnAtSelectIndex(headerIndex)) {
        const whereExpression = parsedQuery.getWhereExpression();
        if (whereExpression && whereExpression.containsColumn(header)) {
          basicActions.push({
            icon: IconNames.FILTER_REMOVE,
            title: `Remove from WHERE clause`,
            onAction: () => {
              onQueryChange(
                parsedQuery.changeWhereExpression(whereExpression.removeColumnFromAnd(header)),
                true,
              );
            },
          });
        }

        const havingExpression = parsedQuery.getHavingExpression();
        if (havingExpression && havingExpression.containsColumn(header)) {
          basicActions.push({
            icon: IconNames.FILTER_REMOVE,
            title: `Remove from HAVING clause`,
            onAction: () => {
              onQueryChange(
                parsedQuery.changeHavingExpression(havingExpression.removeColumnFromAnd(header)),
                true,
              );
            },
          });
        }
      }

      if (!parsedQuery.hasStarInSelect()) {
        basicActions.push({
          icon: IconNames.EDIT,
          title: `Rename column`,
          onAction: () => {
            setRenamingColumn(headerIndex);
          },
        });
      }

      basicActions.push({
        icon: IconNames.CROSS,
        title: `Remove column`,
        onAction: () => {
          onQueryChange(parsedQuery.removeOutputColumn(header), true);
        },
      });

      return basicActionsToMenu(basicActions);
    } else {
      const orderByExpression = SqlRef.column(header);
      const descOrderBy = orderByExpression.toOrderByPart('DESC');
      const ascOrderBy = orderByExpression.toOrderByPart('ASC');
      const descOrderByPretty = prettyPrintSql(descOrderBy);
      const ascOrderByPretty = prettyPrintSql(descOrderBy);
      return (
        <Menu>
          <MenuItem
            icon={IconNames.CLIPBOARD}
            text={`Copy: ${prettyRef}`}
            onClick={() => {
              copyAndAlert(String(ref), `${prettyRef}' copied to clipboard`);
            }}
          />
          {!runeMode && (
            <>
              <MenuItem
                icon={IconNames.CLIPBOARD}
                text={`Copy: ${descOrderByPretty}`}
                onClick={() =>
                  copyAndAlert(descOrderBy.toString(), `'${descOrderByPretty}' copied to clipboard`)
                }
              />
              <MenuItem
                icon={IconNames.CLIPBOARD}
                text={`Copy: ${ascOrderByPretty}`}
                onClick={() =>
                  copyAndAlert(ascOrderBy.toString(), `'${ascOrderByPretty}' copied to clipboard`)
                }
              />
            </>
          )}
        </Menu>
      );
    }
  }

  function filterOnMenuItem(icon: IconName, clause: SqlExpression, having: boolean) {
    const { onQueryChange } = props;
    if (!parsedQuery) return;

    return (
      <MenuItem
        icon={icon}
        text={`${having ? 'Having' : 'Filter on'}: ${prettyPrintSql(clause)}`}
        onClick={() => {
          onQueryChange(
            having ? parsedQuery.addHaving(clause) : parsedQuery.addWhere(clause),
            true,
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

  function getCellMenu(header: string, headerIndex: number, value: unknown) {
    const { runeMode } = props;

    const val = SqlLiteral.maybe(value);
    const showFullValueMenuItem = (
      <MenuItem
        icon={IconNames.EYE_OPEN}
        text="Show full value"
        onClick={() => {
          setShowValue(stringifyValue(value));
        }}
      />
    );

    if (parsedQuery) {
      let ex: SqlExpression | undefined;
      let having = false;
      const selectValue = parsedQuery.getSelectExpressionForIndex(headerIndex);
      if (selectValue) {
        const outputName = selectValue.getOutputName();
        having = parsedQuery.isAggregateSelectIndex(headerIndex);
        if (having && outputName) {
          ex = SqlRef.column(outputName);
        } else {
          ex = selectValue.getUnderlyingExpression();
        }
      } else if (parsedQuery.hasStarInSelect()) {
        ex = SqlRef.column(header);
      }

      return (
        <Menu>
          {ex && val && (
            <>
              {isComparable(value) && (
                <>
                  {filterOnMenuItem(IconNames.FILTER_KEEP, ex.greaterThanOrEqual(val), having)}
                  {filterOnMenuItem(IconNames.FILTER_KEEP, ex.lessThanOrEqual(val), having)}
                </>
              )}
              {filterOnMenuItem(IconNames.FILTER_KEEP, ex.equal(val), having)}
              {filterOnMenuItem(IconNames.FILTER_REMOVE, ex.unequal(val), having)}
            </>
          )}
          {showFullValueMenuItem}
        </Menu>
      );
    } else {
      const ref = SqlRef.column(header);
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

  function getHeaderClassName(header: string, i: number) {
    if (!parsedQuery) return;

    const className = [];
    const orderBy = parsedQuery.getOrderByForOutputColumn(header);
    if (orderBy) {
      className.push(orderBy.getEffectiveDirection() === 'DESC' ? '-sort-desc' : '-sort-asc');
    }

    if (parsedQuery.isAggregateOutputColumn(header)) {
      className.push('aggregate-header');
    }

    if (i === renamingColumn) {
      className.push('renaming');
    }

    return className.join(' ');
  }

  function renameColumnTo(renameTo: string | undefined) {
    setRenamingColumn(-1);
    if (renameTo && parsedQuery) {
      if (parsedQuery.hasStarInSelect()) return;
      const selectExpression = parsedQuery.getSelectExpressionForIndex(renamingColumn);
      if (!selectExpression) return;
      onQueryChange(parsedQuery.changeSelect(renamingColumn, selectExpression.as(renameTo)), true);
    }
  }

  const outerLimit = queryResult.getSqlOuterLimit();
  const hasMoreResults = queryResult.rows.length === outerLimit;

  function changePagination(pagination: Pagination) {
    if (
      hasMoreResults &&
      Math.floor(queryResult.rows.length / pagination.pageSize) === pagination.page // on the last page
    ) {
      onLoadMore();
    }
    setPagination(pagination);
  }

  const numericColumnBraces = getNumericColumnBraces(queryResult, pagination);
  return (
    <div className={classNames('query-output', { 'more-results': hasMoreResults })}>
      <ReactTable
        data={queryResult.rows as any[][]}
        noDataText={queryResult.rows.length ? '' : 'Query returned no data'}
        page={pagination.page}
        pageSize={pagination.pageSize}
        onPageChange={page => changePagination(changePage(pagination, page))}
        onPageSizeChange={(pageSize, page) => changePagination({ page, pageSize })}
        sortable={false}
        ofText={hasMoreResults ? '' : 'of'}
        columns={queryResult.header.map((column, i) => {
          const h = column.name;
          return {
            Header:
              i === renamingColumn && parsedQuery
                ? () => <ColumnRenameInput initialName={h} onDone={renameColumnTo} />
                : () => {
                    return (
                      <Popover2 className="clickable-cell" content={getHeaderMenu(h, i)}>
                        <div>
                          {h}
                          {hasFilterOnHeader(h, i) && (
                            <Icon icon={IconNames.FILTER} iconSize={14} />
                          )}
                        </div>
                      </Popover2>
                    );
                  },
            headerClassName: getHeaderClassName(h, i),
            accessor: String(i),
            Cell: function QueryOutputTableCell(row) {
              const value = row.value;
              return (
                <div>
                  <Popover2 content={getCellMenu(h, i, value)}>
                    {numericColumnBraces[i] ? (
                      <BracedText
                        text={String(value)}
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
            className:
              parsedQuery && parsedQuery.isAggregateOutputColumn(h)
                ? 'aggregate-column'
                : undefined,
          };
        })}
      />
      {showValue && <ShowValueDialog onClose={() => setShowValue(undefined)} str={showValue} />}
    </div>
  );
});
