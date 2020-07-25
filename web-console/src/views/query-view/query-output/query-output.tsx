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

import { Icon, Menu, MenuItem, Popover } from '@blueprintjs/core';
import { IconName, IconNames } from '@blueprintjs/icons';
import {
  QueryResult,
  SqlExpression,
  SqlLiteral,
  SqlQuery,
  SqlRef,
  trimString,
} from 'druid-query-toolkit';
import React, { useState } from 'react';
import ReactTable from 'react-table';

import { TableCell } from '../../../components';
import { ShowValueDialog } from '../../../dialogs/show-value-dialog/show-value-dialog';
import { copyAndAlert, prettyPrintSql } from '../../../utils';
import { BasicAction, basicActionsToMenu } from '../../../utils/basic-action';

import './query-output.scss';

function isComparable(x: unknown): boolean {
  return x !== null && x !== '' && !isNaN(Number(x));
}

export interface QueryOutputProps {
  loading: boolean;
  queryResult?: QueryResult;
  onQueryChange: (query: SqlQuery, run?: boolean) => void;
  error?: string;
  runeMode: boolean;
}

export const QueryOutput = React.memo(function QueryOutput(props: QueryOutputProps) {
  const { queryResult, loading, error } = props;
  const parsedQuery = queryResult ? queryResult.sqlQuery : undefined;
  const [showValue, setShowValue] = useState();

  function hasFilterOnHeader(header: string, headerIndex: number): boolean {
    if (!parsedQuery || !parsedQuery.isRealOutputColumnAtSelectIndex(headerIndex)) return false;

    return (
      parsedQuery.getEffectiveWhereExpression().containsColumn(header) ||
      parsedQuery.getEffectiveHavingExpression().containsColumn(header)
    );
  }

  function getHeaderMenu(header: string, headerIndex: number) {
    const { onQueryChange, runeMode } = props;
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
            having ? parsedQuery.addToHaving(clause) : parsedQuery.addToWhere(clause),
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

  function getCellMenu(header: string, headerIndex: number, value: any) {
    const { runeMode } = props;

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

    const val = SqlLiteral.create(value);
    if (parsedQuery) {
      const selectValue = parsedQuery.getSelectExpressionForIndex(headerIndex);
      if (selectValue) {
        const outputName = selectValue.getOutputName();
        const having = parsedQuery.isAggregateSelectIndex(headerIndex);
        let ex: SqlExpression;
        if (having && outputName) {
          ex = SqlRef.column(outputName);
        } else {
          ex = selectValue.expression as SqlExpression;
        }

        return (
          <Menu>
            {isComparable(value) && (
              <>
                {filterOnMenuItem(IconNames.FILTER_KEEP, ex.greaterThanOrEqual(val), having)}
                {filterOnMenuItem(IconNames.FILTER_KEEP, ex.lessThanOrEqual(val), having)}
              </>
            )}
            {filterOnMenuItem(IconNames.FILTER_KEEP, ex.equal(val), having)}
            {filterOnMenuItem(IconNames.FILTER_REMOVE, ex.unequal(val), having)}
            {showFullValueMenuItem}
          </Menu>
        );
      }
    }

    const ref = SqlRef.column(header);
    const trimmedValue = trimString(String(value), 50);
    return (
      <Menu>
        <MenuItem
          icon={IconNames.CLIPBOARD}
          text={`Copy: ${trimmedValue}`}
          onClick={() => copyAndAlert(value, `${trimmedValue} copied to clipboard`)}
        />
        {!runeMode && (
          <>
            {clipboardMenuItem(ref.equal(val))}
            {clipboardMenuItem(ref.unequal(val))}
          </>
        )}
        {showFullValueMenuItem}
      </Menu>
    );
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

  return (
    <div className="query-output">
      <ReactTable
        data={queryResult ? (queryResult.rows as any[][]) : []}
        loading={loading}
        noDataText={
          !loading && queryResult && !queryResult.rows.length
            ? 'Query returned no data'
            : error || ''
        }
        sortable={false}
        columns={(queryResult ? queryResult.header : []).map((column, i) => {
          const h = column.name;
          return {
            Header: () => {
              return (
                <Popover className={'clickable-cell'} content={getHeaderMenu(h, i)}>
                  <div>
                    {h}
                    {hasFilterOnHeader(h, i) && <Icon icon={IconNames.FILTER} iconSize={14} />}
                  </div>
                </Popover>
              );
            },
            headerClassName: getHeaderClassName(h),
            accessor: String(i),
            Cell: row => {
              const value = row.value;
              return (
                <div>
                  <Popover content={getCellMenu(h, i, value)}>
                    <TableCell value={value} unlimited />
                  </Popover>
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
