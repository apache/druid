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

import { Menu, MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type { Column, SqlExpression, SqlQuery } from '@druid-toolkit/query';
import { C, L, SqlComparison, SqlLiteral, SqlRecord, trimString } from '@druid-toolkit/query';
import React from 'react';

import type { QueryAction } from '../../utils';
import { copyAndAlert, prettyPrintSql, stringifyValue } from '../../utils';

function sqlLiteralForColumnValue(column: Column, value: unknown): SqlLiteral | undefined {
  if (column.sqlType === 'TIMESTAMP') {
    const asDate = new Date(value as any);
    if (!isNaN(asDate.valueOf())) {
      return L(asDate);
    }
  }

  return SqlLiteral.maybe(value);
}

function isComparable(x: unknown): boolean {
  return x !== null && x !== '';
}

function addToClause(clause: SqlExpression, newValue: SqlLiteral): SqlExpression | undefined {
  if (!(clause instanceof SqlComparison)) return;
  const { op, lhs, rhs } = clause;

  switch (op) {
    case '=':
      if (!(rhs instanceof SqlLiteral)) return;
      if (rhs.equals(newValue)) return;
      return lhs.in([rhs, newValue]);

    case '<>':
      if (!(rhs instanceof SqlLiteral)) return;
      if (rhs.equals(newValue)) return;
      return lhs.notIn([rhs, newValue]);

    case 'IN':
      if (!(rhs instanceof SqlRecord)) return;
      if (rhs.contains(newValue)) return;
      return clause.changeRhs(rhs.prepend(newValue));

    default:
      return;
  }
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

export interface CellFilterMenuProps {
  column: Column;
  value: unknown;
  headerIndex: number;
  runeMode?: boolean;
  query: SqlQuery | undefined;
  onQueryAction?(action: QueryAction): void;
  onShowFullValue?(valueString: string): void;
}

export function CellFilterMenu(props: CellFilterMenuProps) {
  const { column, value, runeMode, headerIndex, query, onQueryAction, onShowFullValue } = props;

  const showFullValueMenuItem = onShowFullValue ? (
    <MenuItem
      icon={IconNames.EYE_OPEN}
      text="Show full value"
      onClick={() => {
        onShowFullValue(stringifyValue(value));
      }}
    />
  ) : undefined;

  const val = sqlLiteralForColumnValue(column, value);

  if (query) {
    let ex: SqlExpression | undefined;
    let having = false;
    if (query.hasStarInSelect()) {
      ex = C(column.name);
    } else {
      const selectValue = query.getSelectExpressionForIndex(headerIndex);
      if (selectValue) {
        const outputName = selectValue.getOutputName();
        having = query.isAggregateSelectIndex(headerIndex);
        if (having && outputName) {
          ex = C(outputName);
        } else {
          ex = selectValue.getUnderlyingExpression();
        }
      }
    }

    const filterOnMenuItem = (clause: SqlExpression) => {
      if (!onQueryAction) return;
      return (
        <MenuItem
          icon={IconNames.FILTER}
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
    };

    const currentFilterExpression = having
      ? query.getHavingExpression()
      : query.getWhereExpression();

    const currentClauses =
      currentFilterExpression
        ?.decomposeViaAnd()
        ?.filter(ex => String(ex.getUsedColumnNames()) === column.name) || [];

    const updatedClause =
      currentClauses.length === 1 && val ? addToClause(currentClauses[0], val) : undefined;

    const jsonColumn = column.nativeType === 'COMPLEX<json>';
    return (
      <Menu>
        {ex?.getFirstColumnName() && val && !jsonColumn && (
          <>
            {updatedClause && filterOnMenuItem(updatedClause)}
            {filterOnMenuItem(ex.equal(val))}
            {filterOnMenuItem(ex.unequal(val))}
            {isComparable(value) && (
              <>
                {filterOnMenuItem(ex.greaterThanOrEqual(val))}
                {filterOnMenuItem(ex.lessThanOrEqual(val))}
              </>
            )}
          </>
        )}
        {showFullValueMenuItem}
      </Menu>
    );
  } else {
    const ref = C(column.name);
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
