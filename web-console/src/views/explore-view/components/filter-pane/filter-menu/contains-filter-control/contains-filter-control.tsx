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

import { FormGroup, InputGroup, Menu, MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type { ContainsFilterPattern, QueryResult, SqlQuery } from 'druid-query-toolkit';
import { C, F, filterPatternToExpression, SqlExpression } from 'druid-query-toolkit';
import React, { useMemo } from 'react';

import { useQueryManager } from '../../../../../../hooks';
import type { QuerySource } from '../../../../models';

import './contains-filter-control.scss';

export interface ContainsFilterControlProps {
  querySource: QuerySource;
  filter: SqlExpression | undefined;
  filterPattern: ContainsFilterPattern;
  setFilterPattern(filterPattern: ContainsFilterPattern): void;
  runSqlQuery(query: string | SqlQuery): Promise<QueryResult>;
}

export const ContainsFilterControl = React.memo(function ContainsFilterControl(
  props: ContainsFilterControlProps,
) {
  const { querySource, filter, filterPattern, setFilterPattern, runSqlQuery } = props;
  const { column, negated, contains } = filterPattern;

  const previewQuery = useMemo(
    () =>
      querySource
        .getInitQuery(
          SqlExpression.and(
            filter,
            contains ? filterPatternToExpression(filterPattern) : undefined,
          ),
        )
        .addSelect(F.cast(C(column), 'VARCHAR').as('c'), { addToGroupBy: 'end' })
        .changeOrderByExpression(F.count().toOrderByExpression('DESC'))
        .changeLimitValue(101)
        .toString(),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- exclude 'makePattern' from deps
    [querySource.query, filter, column, contains, negated],
  );

  const [previewState] = useQueryManager<string, string[]>({
    query: previewQuery,
    debounceIdle: 100,
    debounceLoading: 500,
    processQuery: async query => {
      const vs = await runSqlQuery(query);
      return (vs.getColumnByName('c') || []).map(String);
    },
  });

  return (
    <div className="contains-filter-control">
      <FormGroup>
        <InputGroup
          value={contains}
          onChange={e => setFilterPattern({ ...filterPattern, contains: e.target.value })}
          placeholder="Search string"
        />
      </FormGroup>
      <FormGroup label="Preview">
        <Menu className="preview-list">
          {previewState.data?.map((v, i) => (
            <MenuItem
              key={i}
              className="preview-item"
              text={String(v)}
              shouldDismissPopover={false}
            />
          ))}
          {previewState.loading && <MenuItem disabled text="Loading..." />}
          {previewState.error && (
            <MenuItem icon={IconNames.ERROR} disabled text={previewState.getErrorMessage()} />
          )}
        </Menu>
      </FormGroup>
    </div>
  );
});
