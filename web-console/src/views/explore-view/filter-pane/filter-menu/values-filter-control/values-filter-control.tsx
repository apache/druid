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

import { Button, FormGroup, InputGroup, Intent, Menu, MenuItem } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import type { ValuesFilterPattern } from '@druid-toolkit/query';
import { C, F, L, SqlExpression, SqlLiteral } from '@druid-toolkit/query';
import React, { useMemo, useState } from 'react';

import { useQueryManager } from '../../../../../hooks';
import { caseInsensitiveContains, nonEmptyArray } from '../../../../../utils';
import { ColumnPicker } from '../../../column-picker/column-picker';
import type { Dataset } from '../../../utils';
import { toggle } from '../../../utils';
import { ColumnValue } from '../../column-value/column-value';

import './values-filter-control.scss';

export interface ValuesFilterControlProps {
  dataset: Dataset;
  filter: SqlExpression | undefined;
  initFilterPattern: ValuesFilterPattern;
  negated: boolean;
  setFilterPattern(filterPattern: ValuesFilterPattern): void;
  onClose(): void;
  queryDruidSql<T = any>(sqlQueryPayload: Record<string, any>): Promise<T[]>;
}

export const ValuesFilterControl = React.memo(function ValuesFilterControl(
  props: ValuesFilterControlProps,
) {
  const { dataset, filter, initFilterPattern, negated, setFilterPattern, onClose, queryDruidSql } =
    props;
  const [column, setColumn] = useState<string>(initFilterPattern.column);
  const [selectedValues, setSelectedValues] = useState<any[]>(initFilterPattern.values);
  const [searchString, setSearchString] = useState('');

  function makePattern(): ValuesFilterPattern {
    return {
      type: 'values',
      negated,
      column,
      values: selectedValues,
    };
  }

  const valuesQuery = useMemo(() => {
    const columnRef = C(column);
    const queryParts: string[] = [`SELECT ${columnRef.as('c')}`, `FROM ${dataset.table}`];

    const filterEx = SqlExpression.and(
      filter,
      searchString ? F('ICONTAINS_STRING', columnRef, L(searchString)) : undefined,
    );
    if (!(filterEx instanceof SqlLiteral)) {
      queryParts.push(`WHERE ${filterEx}`);
    }

    queryParts.push(`GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 101`);
    return queryParts.join('\n');
  }, [dataset.table, filter, column, searchString]);

  const [valuesState] = useQueryManager<string, any[]>({
    query: valuesQuery,
    debounceIdle: 100,
    debounceLoading: 500,
    processQuery: async query => {
      const vs = await queryDruidSql<{ c: any }>({
        query,
      });

      return vs.map(d => d.c);
    },
  });

  const filterPatternValues = initFilterPattern.values;
  let valuesToShow: any[] = filterPatternValues;
  const values = valuesState.data;
  if (values) {
    valuesToShow = valuesToShow.concat(values.filter(v => !filterPatternValues.includes(v)));
  }
  if (searchString) {
    valuesToShow = valuesToShow.filter(v => caseInsensitiveContains(v, searchString));
  }

  return (
    <div className="values-filter-control">
      <FormGroup label="Column">
        <ColumnPicker
          availableColumns={dataset.columns}
          selectedColumnName={column}
          onSelectedColumnNameChange={selectedColumnName => {
            setColumn(selectedColumnName);
            setSelectedValues([]);
          }}
        />
      </FormGroup>
      <FormGroup>
        <InputGroup
          value={searchString}
          onChange={e => setSearchString(e.target.value)}
          placeholder="Search..."
        />
        <Menu className="value-list">
          {valuesToShow.map((v, i) => (
            <MenuItem
              key={i}
              icon={
                selectedValues.includes(v)
                  ? negated
                    ? IconNames.DELETE
                    : IconNames.FULL_CIRCLE
                  : IconNames.CIRCLE
              }
              text={<ColumnValue value={v} />}
              shouldDismissPopover={false}
              onClick={e => {
                setSelectedValues(e.altKey ? [v] : toggle(selectedValues, v));
              }}
            />
          ))}
          {valuesState.loading && (
            <MenuItem icon={IconNames.BLANK} disabled>
              Loading...
            </MenuItem>
          )}
        </Menu>
      </FormGroup>
      <div className="button-bar">
        <Button
          intent={Intent.PRIMARY}
          text="OK"
          onClick={() => {
            const newPattern = makePattern();
            if (nonEmptyArray(newPattern.values)) {
              setFilterPattern(newPattern);
            } else {
              onClose();
            }
          }}
        />
      </div>
    </div>
  );
});
