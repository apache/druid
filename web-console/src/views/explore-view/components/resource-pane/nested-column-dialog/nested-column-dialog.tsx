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

import { Button, Classes, Dialog, Intent, Menu } from '@blueprintjs/core';
import type { SqlExpression } from '@druid-toolkit/query';
import { type QueryResult, F, sql, SqlQuery } from '@druid-toolkit/query';
import React, { useState } from 'react';

import { MenuCheckbox } from '../../../../../components';
import { useQueryManager } from '../../../../../hooks';
import { pluralIfNeeded, uniq } from '../../../../../utils';
import { QuerySource } from '../../../models';
import { toggle } from '../../../utils';

import './nested-column-dialog.scss';

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
  const { nestedColumn, querySource, runSqlQuery, onClose } = props;
  const [selectedPaths, setSelectedPaths] = useState<string[]>([]);

  const [pathsState] = useQueryManager({
    query: nestedColumn,
    processQuery: async nestedColumn => {
      const query = SqlQuery.from(QuerySource.stripToBaseSource(querySource.query))
        .addSelect(F('JSON_PATHS', nestedColumn).as('p'))
        .applyIf(querySource.hasBaseTimeColumn(), q =>
          q.addWhere(sql`MAX_DATA_TIME() - INTERVAL '14' DAY <= __time`),
        )
        .changeLimitValue(200);

      const pathResult = await runSqlQuery(query);

      const pathColumn = pathResult.getColumnByIndex(0);
      if (!pathColumn) throw new Error('Could not get path column');

      return uniq(([] as string[]).concat(...pathColumn)).sort();
    },
  });

  const paths = pathsState.data;
  return (
    <Dialog className="nested-column-dialog" isOpen onClose={onClose} title="Expand nested column">
      <div className={Classes.DIALOG_BODY}>
        {pathsState.getErrorMessage()}
        {paths && (
          <Menu>
            {pathsState.data?.map((path, i) => {
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
        )}
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <div className="edit-column-dialog-buttons">
            <Button text="Cancel" onClick={onClose} />
            <Button
              text={
                selectedPaths.length
                  ? `Explode into ${pluralIfNeeded(selectedPaths.length, 'column')}`
                  : 'Select path'
              }
              disabled={!selectedPaths.length}
              intent={Intent.PRIMARY}
              onClick={() => {
                onClose();
              }}
            />
          </div>
        </div>
      </div>
    </Dialog>
  );
});
