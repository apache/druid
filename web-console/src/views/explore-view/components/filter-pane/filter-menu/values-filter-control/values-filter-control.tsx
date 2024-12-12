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

import { FormGroup, Menu, MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type { QueryResult, SqlQuery, ValuesFilterPattern } from 'druid-query-toolkit';
import { C, F, SqlExpression } from 'druid-query-toolkit';
import React, { useMemo, useState } from 'react';

import { ClearableInput } from '../../../../../../components';
import { useQueryManager } from '../../../../../../hooks';
import { caseInsensitiveContains, filterMap } from '../../../../../../utils';
import type { QuerySource } from '../../../../models';
import { toggle } from '../../../../utils';
import { ColumnValue } from '../../column-value/column-value';

import './values-filter-control.scss';

export interface ValuesFilterControlProps {
  querySource: QuerySource;
  filter: SqlExpression | undefined;
  filterPattern: ValuesFilterPattern;
  setFilterPattern(filterPattern: ValuesFilterPattern): void;
  runSqlQuery(query: string | SqlQuery): Promise<QueryResult>;
}

export const ValuesFilterControl = React.memo(function ValuesFilterControl(
  props: ValuesFilterControlProps,
) {
  const { querySource, filter, filterPattern, setFilterPattern, runSqlQuery } = props;
  const { column, negated, values: selectedValues } = filterPattern;
  const [initValues] = useState(selectedValues);
  const [searchString, setSearchString] = useState('');

  const valuesQuery = useMemo(
    () =>
      querySource
        .getInitQuery(
          SqlExpression.and(
            filter,
            searchString ? F('ICONTAINS_STRING', C(column), searchString) : undefined,
          ),
        )
        .addSelect(C(column).as('c'), { addToGroupBy: 'end' })
        .changeOrderByExpression(F.count().toOrderByExpression('DESC'))
        .changeLimitValue(101)
        .toString(),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [querySource.query, filter, column, searchString],
  );

  const [valuesState] = useQueryManager<string, any[]>({
    query: valuesQuery,
    debounceIdle: 100,
    debounceLoading: 500,
    processQuery: async query => {
      const vs = await runSqlQuery(query);
      return vs.getColumnByName('c') || [];
    },
  });

  let valuesToShow: any[] = initValues;
  const values = valuesState.data;
  if (values) {
    valuesToShow = valuesToShow.concat(values.filter(v => !initValues.includes(v)));
  }

  const showSearch = querySource.columns.find(c => c.name === column)?.sqlType !== 'BOOLEAN';
  return (
    <FormGroup className="values-filter-control">
      {showSearch && (
        <ClearableInput value={searchString} onChange={setSearchString} placeholder="Search" />
      )}
      <Menu className="value-list">
        {filterMap(valuesToShow, (v, i) => {
          if (!caseInsensitiveContains(v, searchString)) return;
          return (
            <MenuItem
              key={i}
              icon={
                selectedValues.includes(v)
                  ? negated
                    ? IconNames.DELETE
                    : IconNames.TICK_CIRCLE
                  : IconNames.CIRCLE
              }
              text={<ColumnValue value={v} />}
              shouldDismissPopover={false}
              onClick={e => {
                setFilterPattern({
                  ...filterPattern,
                  values: e.altKey ? [v] : toggle(selectedValues, v),
                });
              }}
            />
          );
        })}
        {valuesState.loading && <MenuItem icon={IconNames.BLANK} text="Loading..." disabled />}
      </Menu>
    </FormGroup>
  );
});
