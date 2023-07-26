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

import { MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type { SqlExpression, SqlQuery } from '@druid-toolkit/query';
import { C, F } from '@druid-toolkit/query';
import type { JSX } from 'react';
import React from 'react';

import { prettyPrintSql } from '../../../../../utils';

const UNIQUE_FUNCTIONS: Record<string, string> = {
  'COMPLEX<hyperUnique>': 'APPROX_COUNT_DISTINCT_BUILTIN',
  'COMPLEX<thetaSketch>': 'APPROX_COUNT_DISTINCT_DS_THETA',
  'COMPLEX<HLLSketch>': 'APPROX_COUNT_DISTINCT_DS_HLL',
};

const QUANTILE_FUNCTIONS: Record<string, string> = {
  'COMPLEX<quantilesDoublesSketch>': 'APPROX_QUANTILE_DS',
};

export interface ComplexMenuItemsProps {
  table: string;
  schema: string;
  columnName: string;
  columnType: string;
  parsedQuery: SqlQuery;
  onQueryChange: (query: SqlQuery, run?: boolean) => void;
}

export const ComplexMenuItems = React.memo(function ComplexMenuItems(props: ComplexMenuItemsProps) {
  const { columnName, columnType, parsedQuery, onQueryChange } = props;
  const column = C(columnName);

  function renderAggregateMenu(): JSX.Element | undefined {
    if (!parsedQuery.hasGroupBy()) return;

    function aggregateMenuItem(ex: SqlExpression, alias: string) {
      return (
        <MenuItem
          text={prettyPrintSql(ex)}
          onClick={() => {
            onQueryChange(parsedQuery.addSelect(ex.as(alias)), true);
          }}
        />
      );
    }

    const uniqueFn = UNIQUE_FUNCTIONS[columnType];
    const quantileFn = QUANTILE_FUNCTIONS[columnType];
    if (!uniqueFn && !quantileFn) return;

    return (
      <MenuItem icon={IconNames.FUNCTION} text="Aggregate">
        {uniqueFn && aggregateMenuItem(F(uniqueFn, column), `unique_${columnName}`)}
        {quantileFn && (
          <>
            {aggregateMenuItem(F(quantileFn, column, 0.5), `median_${columnName}`)}
            {aggregateMenuItem(F(quantileFn, column, 0.98), `p98_${columnName}`)}
          </>
        )}
      </MenuItem>
    );
  }

  return <>{renderAggregateMenu()}</>;
});
