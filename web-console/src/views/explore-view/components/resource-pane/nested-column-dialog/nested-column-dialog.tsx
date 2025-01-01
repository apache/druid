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

import {
  Button,
  ButtonGroup,
  Classes,
  Dialog,
  FormGroup,
  InputGroup,
  Intent,
  Menu,
  Tag,
} from '@blueprintjs/core';
import type { QueryResult, SqlExpression, SqlQuery } from 'druid-query-toolkit';
import { F, sql, SqlFunction } from 'druid-query-toolkit';
import React, { useState } from 'react';

import { ClearableInput, Loader, MenuCheckbox } from '../../../../../components';
import { useQueryManager } from '../../../../../hooks';
import { caseInsensitiveContains, filterMap, pluralIfNeeded } from '../../../../../utils';
import type { QuerySource } from '../../../models';
import { ExpressionMeta } from '../../../models';
import { toggle } from '../../../utils';

import './nested-column-dialog.scss';

const ARRAY_CONCAT_AGG_SIZE = 10000;

export interface NestedColumnDialogProps {
  nestedColumn: SqlExpression;
  onApply(newQuery: SqlQuery): void;
  querySource: QuerySource;
  runSqlQuery(query: string | SqlQuery): Promise<QueryResult>;
  onClose(): void;
}

export const NestedColumnDialog = React.memo(function NestedColumnDialog(
  props: NestedColumnDialogProps,
) {
  const { nestedColumn, onApply, querySource, runSqlQuery, onClose } = props;
  const [searchString, setSearchString] = useState('');
  const [selectedPaths, setSelectedPaths] = useState<string[]>([]);
  const [namingScheme, setNamingScheme] = useState(`${nestedColumn.getFirstColumnName()}[%]`);

  const [pathsState] = useQueryManager({
    query: nestedColumn,
    processQuery: async nestedColumn => {
      const query = querySource
        .getInitBaseQuery()
        .addSelect(
          SqlFunction.decorated('ARRAY_CONCAT_AGG', 'DISTINCT', [
            F('JSON_PATHS', nestedColumn),
            ARRAY_CONCAT_AGG_SIZE,
          ]),
        )
        .applyIf(querySource.hasBaseTimeColumn(), q =>
          q.addWhere(sql`MAX_DATA_TIME() - INTERVAL '14' DAY <= __time`),
        );

      const pathResult = await runSqlQuery(query);

      const paths = pathResult.rows[0]?.[0];
      if (!Array.isArray(paths)) throw new Error('Could not get paths');

      return paths;
    },
  });

  const paths = pathsState.data;
  return (
    <Dialog className="nested-column-dialog" isOpen onClose={onClose} title="Expand nested column">
      <div className={Classes.DIALOG_BODY}>
        <p>
          Replace <Tag minimal>{String(nestedColumn.getOutputName())}</Tag> with path expansions for
          the selected paths.
        </p>
        {pathsState.isLoading() && <Loader />}
        {pathsState.getErrorMessage()}
        {paths && (
          <FormGroup>
            <ClearableInput value={searchString} onChange={setSearchString} placeholder="Search" />
            <Menu className="path-selector">
              {filterMap(paths, (path, i) => {
                if (!caseInsensitiveContains(path, searchString)) return;
                return (
                  <MenuCheckbox
                    key={i}
                    checked={selectedPaths.includes(path)}
                    onChange={() => setSelectedPaths(toggle(selectedPaths, path))}
                    text={path}
                  />
                );
              })}
            </Menu>
            <ButtonGroup fill>
              <Button
                text="Select all"
                onClick={() =>
                  // Select all paths matching the shown search string
                  setSelectedPaths(
                    paths.filter(path => caseInsensitiveContains(path, searchString)),
                  )
                }
              />
              <Button
                text="Select none"
                onClick={() =>
                  // Remove from selection all the paths matching the search string
                  setSelectedPaths(
                    selectedPaths.filter(path => !caseInsensitiveContains(path, searchString)),
                  )
                }
              />
            </ButtonGroup>
          </FormGroup>
        )}
        <FormGroup label="Nameing scheme">
          <InputGroup
            value={namingScheme}
            onChange={e => {
              setNamingScheme(e.target.value.slice(0, ExpressionMeta.MAX_NAME_LENGTH));
            }}
          />
        </FormGroup>
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <div className="edit-column-dialog-buttons">
            <Button text="Cancel" onClick={onClose} />
            <Button
              text={
                selectedPaths.length
                  ? `Add ${pluralIfNeeded(selectedPaths.length, 'column')}`
                  : 'Select path'
              }
              disabled={!selectedPaths.length}
              intent={Intent.PRIMARY}
              onClick={() => {
                const effectiveNamingScheme = namingScheme.includes('%')
                  ? namingScheme
                  : namingScheme + '[%]';
                onApply(
                  querySource.addColumnAfter(
                    nestedColumn.getOutputName()!,
                    ...selectedPaths.map(path =>
                      F('JSON_VALUE', nestedColumn, path).as(
                        querySource.getAvailableName(
                          effectiveNamingScheme.replaceAll('%', path.replace(/^\$\./, '')),
                        ),
                      ),
                    ),
                  ),
                );
                onClose();
              }}
            />
          </div>
        </div>
      </div>
    </Dialog>
  );
});
