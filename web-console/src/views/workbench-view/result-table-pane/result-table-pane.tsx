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
import { IconName, IconNames } from '@blueprintjs/icons';
import { Popover2 } from '@blueprintjs/popover2';
import classNames from 'classnames';
import {
  Column,
  QueryResult,
  SqlAlias,
  SqlExpression,
  SqlFunction,
  SqlLiteral,
  SqlQuery,
  SqlRef,
  SqlStar,
  trimString,
} from 'druid-query-toolkit';
import * as JSONBig from 'json-bigint-native';
import React, { useEffect, useState } from 'react';
import ReactTable from 'react-table';

import { BracedText, Deferred, TableCell } from '../../../components';
import { ShowValueDialog } from '../../../dialogs/show-value-dialog/show-value-dialog';
import {
  computeFlattenExprsForData,
  possibleDruidFormatForValues,
  TIME_COLUMN,
} from '../../../druid-models';
import { SMALL_TABLE_PAGE_SIZE, SMALL_TABLE_PAGE_SIZE_OPTIONS } from '../../../react-table';
import {
  columnToIcon,
  columnToWidth,
  convertToGroupByExpression,
  copyAndAlert,
  filterMap,
  formatNumber,
  getNumericColumnBraces,
  oneOf,
  Pagination,
  prettyPrintSql,
  QueryAction,
  stringifyValue,
  timeFormatToSql,
} from '../../../utils';
import { ExpressionEditorDialog } from '../../sql-data-loader-view/expression-editor-dialog/expression-editor-dialog';
import { TimeFloorMenuItem } from '../time-floor-menu-item/time-floor-menu-item';

import './result-table-pane.scss';

function sqlLiteralForColumnValue(column: Column, value: unknown): SqlLiteral | undefined {
  if (column.sqlType === 'TIMESTAMP') {
    const asDate = new Date(value as any);
    if (!isNaN(asDate.valueOf())) {
      return SqlLiteral.create(asDate);
    }
  }

  return SqlLiteral.maybe(value);
}

const CAST_TARGETS: string[] = ['VARCHAR', 'BIGINT', 'DOUBLE'];

function jsonValue(ex: SqlExpression, path: string): SqlExpression {
  return SqlExpression.parse(`JSON_VALUE(${ex}, ${SqlLiteral.create(path)})`);
}

function getJsonPaths(jsons: Record<string, any>[]): string[] {
  return ['$.'].concat(computeFlattenExprsForData(jsons, 'path', 'include-arrays', true));
}

function isComparable(x: unknown): boolean {
  return x !== null && x !== '';
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

export interface ResultTablePaneProps {
  queryResult: QueryResult;
  onQueryAction(action: QueryAction): void;
  onExport?(): void;
  runeMode: boolean;
  initPageSize?: number;
}

export const ResultTablePane = React.memo(function ResultTablePane(props: ResultTablePaneProps) {
  const { queryResult, onQueryAction, onExport, runeMode, initPageSize } = props;
  const parsedQuery = queryResult.sqlQuery;
  const [pagination, setPagination] = useState<Pagination>({
    page: 0,
    pageSize: initPageSize || 20,
  });
  const [showValue, setShowValue] = useState<string>();
  const [editingColumn, setEditingColumn] = useState<number>(-1);

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

  function getHeaderMenu(column: Column, headerIndex: number) {
    const header = column.name;
    const type = column.sqlType || column.nativeType;
    const ref = SqlRef.column(header);
    const prettyRef = prettyPrintSql(ref);

    const menuItems: JSX.Element[] = [];
    if (parsedQuery) {
      const noStar = !parsedQuery.hasStarInSelect();
      const selectExpression = parsedQuery.getSelectExpressionForIndex(headerIndex);

      const orderByExpression = parsedQuery.isValidSelectIndex(headerIndex)
        ? SqlLiteral.index(headerIndex)
        : SqlRef.column(header);
      const descOrderBy = orderByExpression.toOrderByExpression('DESC');
      const ascOrderBy = orderByExpression.toOrderByExpression('ASC');
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
              onQueryAction(q => q.changeOrderByExpressions([reverseOrderBy]));
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
              onQueryAction(q => q.changeOrderByExpressions([descOrderBy]));
            }}
          />,
          <MenuItem
            key="order_asc"
            icon={IconNames.SORT_ASC}
            text="Order ascending"
            onClick={() => {
              onQueryAction(q => q.changeOrderByExpressions([ascOrderBy]));
            }}
          />,
        );
      }

      // Casts
      if (selectExpression) {
        const underlyingExpression = selectExpression.getUnderlyingExpression();
        if (
          underlyingExpression instanceof SqlFunction &&
          underlyingExpression.getEffectiveFunctionName() === 'CAST'
        ) {
          menuItems.push(
            <MenuItem
              key="uncast"
              icon={IconNames.CROSS}
              text="Remove cast"
              onClick={() => {
                if (!selectExpression || !underlyingExpression) return;
                onQueryAction(q =>
                  q.changeSelect(
                    headerIndex,
                    underlyingExpression.getArg(0)!.as(selectExpression.getOutputName()),
                  ),
                );
              }}
            />,
          );
        }

        menuItems.push(
          <MenuItem key="cast" icon={IconNames.EXCHANGE} text="Cast to...">
            {filterMap(CAST_TARGETS, asType => {
              if (asType === column.sqlType) return;
              return (
                <MenuItem
                  key={asType}
                  text={asType}
                  onClick={() => {
                    if (!selectExpression) return;
                    onQueryAction(q =>
                      q.changeSelect(
                        headerIndex,
                        selectExpression
                          .getUnderlyingExpression()
                          .cast(asType)
                          .as(selectExpression.getOutputName()),
                      ),
                    );
                  }}
                />
              );
            })}
          </MenuItem>,
        );
      }

      // JSON hint
      if (selectExpression && column.nativeType === 'COMPLEX<json>') {
        const paths = getJsonPaths(
          filterMap(queryResult.rows, row => {
            const v = row[headerIndex];
            // Strangely multi-stage-query-engine and broker deal with JSON differently
            if (v && typeof v === 'object') return v;
            try {
              return JSONBig.parse(v);
            } catch {
              return;
            }
          }),
        );

        if (paths.length) {
          menuItems.push(
            <MenuItem key="json_value" icon={IconNames.DIAGRAM_TREE} text="Get JSON value for...">
              {paths.map(path => {
                return (
                  <MenuItem
                    key={path}
                    text={path}
                    onClick={() => {
                      if (!selectExpression) return;
                      onQueryAction(q =>
                        q.addSelect(
                          jsonValue(selectExpression.getUnderlyingExpression(), path).as(
                            selectExpression.getOutputName() + path.replace(/^\$/, ''),
                          ),
                          { insertIndex: headerIndex + 1 },
                        ),
                      );
                    }}
                  />
                );
              })}
            </MenuItem>,
          );
        }
      }

      if (parsedQuery.isRealOutputColumnAtSelectIndex(headerIndex)) {
        const whereExpression = parsedQuery.getWhereExpression();
        if (whereExpression && whereExpression.containsColumn(header)) {
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
        if (havingExpression && havingExpression.containsColumn(header)) {
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

      if (noStar) {
        menuItems.push(
          <MenuItem
            key="edit_column"
            icon={IconNames.EDIT}
            text="Edit column"
            onClick={() => {
              setEditingColumn(headerIndex);
            }}
          />,
        );
      }

      if (noStar && selectExpression) {
        if (column.isTimeColumn()) {
          menuItems.push(
            <TimeFloorMenuItem
              key="time_floor"
              expression={selectExpression}
              onChange={expression => {
                onQueryAction(q => q.changeSelect(headerIndex, expression));
              }}
            />,
          );
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

          if (parsedQuery.hasGroupBy()) {
            if (parsedQuery.isGroupedOutputColumn(header)) {
              const convertToAggregate = (aggregate: SqlExpression) => {
                onQueryAction(q =>
                  q.removeOutputColumn(header).addSelect(aggregate, {
                    insertIndex: 'last',
                  }),
                );
              };

              const underlyingSelectExpression = selectExpression.getUnderlyingExpression();

              menuItems.push(
                <MenuItem
                  key="convert_to_aggregate"
                  icon={IconNames.EXCHANGE}
                  text="Convert to aggregate"
                >
                  {oneOf(type, 'LONG', 'FLOAT', 'DOUBLE', 'BIGINT') && (
                    <>
                      <MenuItem
                        text="Convert to SUM(...)"
                        onClick={() => {
                          convertToAggregate(
                            SqlFunction.simple('SUM', [underlyingSelectExpression]).as(
                              `sum_${header}`,
                            ),
                          );
                        }}
                      />
                      <MenuItem
                        text="Convert to MIN(...)"
                        onClick={() => {
                          convertToAggregate(
                            SqlFunction.simple('MIN', [underlyingSelectExpression]).as(
                              `min_${header}`,
                            ),
                          );
                        }}
                      />
                      <MenuItem
                        text="Convert to MAX(...)"
                        onClick={() => {
                          convertToAggregate(
                            SqlFunction.simple('MAX', [underlyingSelectExpression]).as(
                              `max_${header}`,
                            ),
                          );
                        }}
                      />
                    </>
                  )}
                  <MenuItem
                    text="Convert to COUNT(DISTINCT ...)"
                    onClick={() => {
                      convertToAggregate(
                        SqlFunction.decorated('COUNT', 'DISTINCT', [underlyingSelectExpression]).as(
                          `unique_${header}`,
                        ),
                      );
                    }}
                  />
                  <MenuItem
                    text="Convert to APPROX_COUNT_DISTINCT_DS_HLL(...)"
                    onClick={() => {
                      convertToAggregate(
                        SqlFunction.simple('APPROX_COUNT_DISTINCT_DS_HLL', [
                          underlyingSelectExpression,
                        ]).as(`unique_${header}`),
                      );
                    }}
                  />
                  <MenuItem
                    text="Convert to APPROX_COUNT_DISTINCT_DS_THETA(...)"
                    onClick={() => {
                      convertToAggregate(
                        SqlFunction.simple('APPROX_COUNT_DISTINCT_DS_THETA', [
                          underlyingSelectExpression,
                        ]).as(`unique_${header}`),
                      );
                    }}
                  />
                </MenuItem>,
              );
            } else {
              const groupByExpression = convertToGroupByExpression(selectExpression);
              if (groupByExpression) {
                menuItems.push(
                  <MenuItem
                    key="convert_to_group_by"
                    icon={IconNames.EXCHANGE}
                    text="Convert to group by"
                    onClick={() => {
                      onQueryAction(q =>
                        q.removeOutputColumn(header).addSelect(groupByExpression, {
                          insertIndex: 'last-grouping',
                          addToGroupBy: 'end',
                        }),
                      );
                    }}
                  />,
                );
              }
            }
          }
        }
      }

      if (noStar) {
        menuItems.push(
          <MenuItem
            key="remove_column"
            icon={IconNames.CROSS}
            text="Remove column"
            onClick={() => {
              onQueryAction(q => q.removeOutputColumn(header));
            }}
          />,
        );
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
        const orderByExpression = SqlRef.column(header);
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
          const column = clause.getUsedColumns()[0];
          onQueryAction(
            having
              ? q => q.removeFromHaving(column).addHaving(clause)
              : q => q.removeColumnFromWhere(column).addWhere(clause),
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
          setShowValue(stringifyValue(value));
        }}
      />
    );

    const val = sqlLiteralForColumnValue(column, value);

    if (parsedQuery) {
      let ex: SqlExpression | undefined;
      let having = false;
      if (parsedQuery.hasStarInSelect()) {
        ex = SqlRef.column(column.name);
      } else {
        const selectValue = parsedQuery.getSelectExpressionForIndex(headerIndex);
        if (selectValue) {
          const outputName = selectValue.getOutputName();
          having = parsedQuery.isAggregateSelectIndex(headerIndex);
          if (having && outputName) {
            ex = SqlRef.column(outputName);
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

  const editingExpression: SqlExpression | undefined =
    parsedQuery && editingColumn !== -1
      ? parsedQuery.getSelectExpressionForIndex(editingColumn)
      : undefined;

  const numericColumnBraces = getNumericColumnBraces(queryResult, pagination);
  return (
    <div className={classNames('result-table-pane', { 'more-results': hasMoreResults })}>
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
          columns={filterMap(queryResult.header, (column, i) => {
            const h = column.name;
            const icon = columnToIcon(column);

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
                      {parsedQuery && (
                        <div className="formula">{getExpressionIfAlias(parsedQuery, i)}</div>
                      )}
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
              className:
                parsedQuery && parsedQuery.isAggregateOutputColumn(h)
                  ? 'aggregate-column'
                  : undefined,
            };
          })}
        />
      )}
      {showValue && <ShowValueDialog onClose={() => setShowValue(undefined)} str={showValue} />}
      {editingExpression && (
        <ExpressionEditorDialog
          includeOutputName
          expression={editingExpression}
          onSave={newExpression => {
            if (!parsedQuery) return;
            onQueryAction(q => q.changeSelect(editingColumn, newExpression));
          }}
          onClose={() => setEditingColumn(-1)}
        />
      )}
    </div>
  );
});
