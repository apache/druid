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
import type { QueryResult, SqlQuery, ValuesFilterPattern } from '@druid-toolkit/query';
import { C, F, L, SqlExpression, SqlLiteral } from '@druid-toolkit/query';
import React, { useMemo, useState } from 'react';

import { useQueryManager } from '../../../../../../hooks';
import { caseInsensitiveContains } from '../../../../../../utils';
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

  const valuesQuery = useMemo(() => {
    const columnRef = C(column);
    const queryParts: string[] = [`SELECT ${columnRef.as('c')}`, `FROM (${querySource.query})`];

    const filterEx = SqlExpression.and(
      filter,
      searchString ? F('ICONTAINS_STRING', columnRef, L(searchString)) : undefined,
    );
    if (!(filterEx instanceof SqlLiteral)) {
      queryParts.push(`WHERE ${filterEx}`);
    }

    queryParts.push(`GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 101`);
    return queryParts.join('\n');
  }, [querySource.query, filter, column, searchString]);

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
  if (searchString) {
    valuesToShow = valuesToShow.filter(v => caseInsensitiveContains(v, searchString));
  }

  const showSearch = querySource.columns.find(c => c.name === column)?.sqlType !== 'BOOLEAN';

  return (
    <FormGroup className="values-filter-control">
      {showSearch && (
        <InputGroup
          value={searchString}
          onChange={e => setSearchString(e.target.value)}
          placeholder="Search"
        />
      )}
      <Menu className="value-list">
        {valuesToShow.map((v, i) => (
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
        ))}
        {valuesState.loading && <MenuItem icon={IconNames.BLANK} text="Loading..." disabled />}
      </Menu>
    </FormGroup>
  );
});
