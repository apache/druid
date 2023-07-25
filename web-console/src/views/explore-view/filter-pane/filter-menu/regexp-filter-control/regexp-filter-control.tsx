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
import type { RegexpFilterPattern } from '@druid-toolkit/query';
import { C, filterPatternToExpression, SqlExpression, SqlLiteral } from '@druid-toolkit/query';
import React, { useMemo, useState } from 'react';

import { useQueryManager } from '../../../../../hooks';
import { ColumnPicker } from '../../../column-picker/column-picker';
import type { Dataset } from '../../../utils';

import './regexp-filter-control.scss';

function regexpIssue(possibleRegexp: string): string | undefined {
  try {
    new RegExp(possibleRegexp);
    return;
  } catch (e) {
    return e.message;
  }
}

export interface RegexpFilterControlProps {
  dataset: Dataset;
  filter: SqlExpression | undefined;
  initFilterPattern: RegexpFilterPattern;
  negated: boolean;
  setFilterPattern(filterPattern: RegexpFilterPattern): void;
  queryDruidSql<T = any>(sqlQueryPayload: Record<string, any>): Promise<T[]>;
}

export const RegexpFilterControl = React.memo(function RegexpFilterControl(
  props: RegexpFilterControlProps,
) {
  const { dataset, filter, initFilterPattern, negated, setFilterPattern, queryDruidSql } = props;
  const [column, setColumn] = useState<string>(initFilterPattern.column);
  const [regexp, setRegexp] = useState(initFilterPattern.regexp);

  function makePattern(): RegexpFilterPattern {
    return {
      type: 'regexp',
      negated,
      column,
      regexp: regexpIssue(regexp) ? '' : regexp,
    };
  }

  const previewQuery = useMemo(() => {
    const columnRef = C(column);
    const queryParts: string[] = [`SELECT ${columnRef.as('c')}`, `FROM ${dataset.table}`];

    const filterEx = SqlExpression.and(
      filter,
      regexp ? filterPatternToExpression(makePattern()) : undefined,
    );
    if (!(filterEx instanceof SqlLiteral)) {
      queryParts.push(`WHERE ${filterEx}`);
    }

    queryParts.push(`GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 101`);
    return queryParts.join('\n');
    // eslint-disable-next-line react-hooks/exhaustive-deps -- exclude 'makePattern' from deps
  }, [dataset.table, filter, column, regexp, negated]);

  const [previewState] = useQueryManager<string, string[]>({
    query: previewQuery,
    debounceIdle: 100,
    debounceLoading: 500,
    processQuery: async query => {
      const vs = await queryDruidSql<{ c: any }>({
        query,
      });

      return vs.map(d => String(d.c));
    },
  });

  const issue = regexpIssue(regexp);
  return (
    <div className="regexp-filter-control">
      <FormGroup label="Column">
        <ColumnPicker
          availableColumns={dataset.columns}
          selectedColumnName={column}
          onSelectedColumnNameChange={setColumn}
        />
      </FormGroup>
      <FormGroup>
        <InputGroup value={regexp} onChange={e => setRegexp(e.target.value)} placeholder="Regexp" />
      </FormGroup>
      <FormGroup label="Preview">
        <Menu className="preview-list">
          {issue ? (
            <MenuItem disabled text={`Invalid regexp: ${issue}`} />
          ) : (
            <>
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
            </>
          )}
        </Menu>
      </FormGroup>
      <div className="button-bar">
        <Button
          intent={Intent.PRIMARY}
          text="OK"
          onClick={() => {
            const newPattern = makePattern();
            // TODO: check if pattern is valid
            // if (!isFilterPatternValid(newPattern)) return;
            setFilterPattern(newPattern);
          }}
        />
      </div>
    </div>
  );
});
