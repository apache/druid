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

import { Button, ButtonGroup } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type { CancelToken } from 'axios';
import classNames from 'classnames';
import type { LiteralValue, QueryResult, SqlQuery, ValuesFilterPattern } from 'druid-query-toolkit';
import {
  F,
  filterPatternsToExpression,
  fitFilterPatterns,
  SqlExpression,
} from 'druid-query-toolkit';
import React, { useMemo, useState } from 'react';

import { ClearableInput, Loader } from '../../../../components';
import { useQueryManager } from '../../../../hooks';
import type { NumberLike } from '../../../../utils';
import {
  caseInsensitiveContains,
  checkedCircleIcon,
  filterMap,
  formatNumber,
  toggle,
  without,
  xor,
} from '../../../../utils';
import type { ExpressionMeta, QuerySource } from '../../models';
import { addOrUpdatePattern } from '../../utils';
import { ColumnValue } from '../column-value/column-value';

import './helper-table.scss';

const ALWAYS_SHOW_CHECKS = true;
const HEADER_HEIGHT = 30;
const ROW_HEIGHT = 28;

export interface HelperTableProps {
  querySource: QuerySource;
  where: SqlExpression;
  setWhere(where: SqlExpression): void;
  expression: ExpressionMeta;
  runSqlQuery(query: string | SqlQuery, cancelToken?: CancelToken): Promise<QueryResult>;
  onDelete(): void;
}

export const HelperTable = React.memo(function HelperTable(props: HelperTableProps) {
  const { querySource, where, setWhere, expression, runSqlQuery, onDelete } = props;
  const [showSearch, setShowSearch] = useState(false);
  const [searchString, setSearchString] = useState('');

  const { patterns, myFilterPattern } = useMemo(() => {
    const patterns = fitFilterPatterns(where);
    const myFilterPattern = patterns.findLast(
      pattern => pattern.type === 'values' && pattern.column === expression.name,
    ) as ValuesFilterPattern | undefined;

    return { patterns, myFilterPattern };
  }, [where, expression]);

  const valuesQuery = useMemo(
    () =>
      querySource
        .getInitQuery(
          SqlExpression.and(
            filterPatternsToExpression(without(patterns, myFilterPattern)),
            searchString ? F('ICONTAINS_STRING', expression.expression, searchString) : undefined,
          ),
        )
        .addSelect(expression.expression.as('v'), { addToGroupBy: 'end' })
        .addSelect(F.count().as('c'), { addToOrderBy: 'end', direction: 'DESC' })
        .changeLimitValue(101)
        .toString(),
    [querySource, patterns, myFilterPattern, expression, searchString],
  );

  const [valuesState] = useQueryManager<string, readonly { v: string; c: NumberLike }[]>({
    query: valuesQuery,
    debounceIdle: 100,
    debounceLoading: 500,
    processQuery: async (query, cancelToken) => {
      const vs = await runSqlQuery(query, cancelToken);
      return (vs.toObjectArray() as any) || [];
    },
  });

  function changeValues(values: LiteralValue[], negatedOverride?: boolean) {
    setWhere(
      filterPatternsToExpression(
        values.length
          ? addOrUpdatePattern(patterns, myFilterPattern, {
              type: 'values',
              column: expression.name,
              negated: negatedOverride ?? Boolean(myFilterPattern?.negated),
              values,
            })
          : patterns.filter(p => p !== myFilterPattern),
      ),
    );
  }

  const values = valuesState.getSomeData();
  const showCheckIcons = Boolean(ALWAYS_SHOW_CHECKS || myFilterPattern);
  return (
    <div
      className="helper-table"
      style={{ maxHeight: values ? HEADER_HEIGHT + ROW_HEIGHT * values.length : undefined }}
    >
      <div className="helper-header">
        <div className="helper-title">{expression.name}</div>
        <ButtonGroup minimal>
          <Button
            icon={IconNames.SEARCH}
            data-tooltip="Search values"
            minimal
            active={showSearch}
            onClick={() => setShowSearch(!showSearch)}
          />
          <Button icon={IconNames.CROSS} data-tooltip="Remove table" minimal onClick={onDelete} />
        </ButtonGroup>
      </div>
      {showSearch && (
        <ClearableInput value={searchString} onValueChange={setSearchString} placeholder="Search" />
      )}
      <div className="values-container">
        {values && (
          <div className="values">
            {filterMap(values, (d, i) => {
              if (!caseInsensitiveContains(d.v, searchString)) return;

              return (
                <div
                  className={classNames('row', { 'with-filter': showCheckIcons })}
                  key={i}
                  onClick={e => {
                    e.preventDefault();
                    changeValues(
                      !e.shiftKey || !myFilterPattern ? [d.v] : toggle(myFilterPattern.values, d.v),
                      false,
                    );
                  }}
                >
                  {showCheckIcons && (
                    <Button
                      className="filter-indicator"
                      icon={checkedCircleIcon(
                        ALWAYS_SHOW_CHECKS
                          ? !myFilterPattern ||
                              xor(myFilterPattern.negated, myFilterPattern.values.includes(d.v))
                          : Boolean(myFilterPattern && myFilterPattern.values.includes(d.v)),
                        !ALWAYS_SHOW_CHECKS && Boolean(myFilterPattern?.negated),
                      )}
                      small
                      minimal
                      onClick={e => {
                        e.stopPropagation();
                        if (myFilterPattern) {
                          changeValues(toggle(myFilterPattern.values, d.v));
                        } else {
                          changeValues([d.v], true);
                        }
                      }}
                    />
                  )}
                  <ColumnValue value={d.v} />
                  <div className="value">{formatNumber(d.c)}</div>
                </div>
              );
            })}
          </div>
        )}
        {valuesState.loading && <Loader />}
      </div>
    </div>
  );
});
