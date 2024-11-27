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

import { Button, Classes, Dialog, FormGroup, InputGroup, Intent, Tag } from '@blueprintjs/core';
import type { QueryResult, SqlQuery } from 'druid-query-toolkit';
import { F, sql, SqlExpression } from 'druid-query-toolkit';
import React, { useMemo, useState } from 'react';

import { AppToaster } from '../../../../../singletons';
import type { QuerySource } from '../../../models';
import { ExpressionMeta } from '../../../models';
import type { Rename } from '../../../utils';
import { PreviewPane } from '../../preview-pane/preview-pane';
import { SqlInput } from '../../sql-input/sql-input';

import './column-dialog.scss';

export interface ColumnDialogProps {
  initExpression: SqlExpression | undefined;
  columnToDuplicate?: string;
  onApply(newQuery: SqlQuery, rename: Rename | undefined): void;
  querySource: QuerySource;
  runSqlQuery(query: string | SqlQuery): Promise<QueryResult>;
  onClose(): void;
}

export const ColumnDialog = React.memo(function ColumnDialog(props: ColumnDialogProps) {
  const { initExpression, columnToDuplicate, onApply, querySource, runSqlQuery, onClose } = props;

  const [outputName, setOutputName] = useState(initExpression?.getOutputName() || '');
  const [formula, setFormula] = useState(String(initExpression?.getUnderlyingExpression() || ''));

  const previewQuery = useMemo(() => {
    const expression = SqlExpression.maybeParse(formula);
    if (!expression) return;
    return querySource
      .getInitBaseQuery()
      .addSelect(F.cast(expression, 'VARCHAR').as('v'), { addToGroupBy: 'end' })
      .applyIf(querySource.hasBaseTimeColumn(), q =>
        q.addWhere(sql`MAX_DATA_TIME() - INTERVAL '14' DAY <= __time`),
      )
      .changeLimitValue(100)
      .toString();
  }, [querySource, formula]);

  return (
    <Dialog
      className="column-dialog"
      isOpen
      onClose={onClose}
      title={initExpression ? 'Edit column' : 'Add column'}
    >
      <div className={Classes.DIALOG_BODY}>
        <div className="controls">
          <FormGroup label="Name">
            <InputGroup
              value={outputName}
              onChange={e => {
                setOutputName(e.target.value.slice(0, ExpressionMeta.MAX_NAME_LENGTH));
              }}
              placeholder="Column name"
            />
          </FormGroup>
          <FormGroup label="SQL expression" className="sql-expression-form-group">
            <SqlInput
              value={formula}
              onValueChange={formula => {
                setFormula(formula);
              }}
              columns={querySource.baseColumns}
              placeholder="SQL expression"
              editorHeight={400}
              autoFocus
              showGutter={false}
            />
          </FormGroup>
        </div>
        <PreviewPane previewQuery={previewQuery} runSqlQuery={runSqlQuery} />
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <div className="edit-column-dialog-buttons">
            <Button text="Cancel" onClick={onClose} />
            <Button
              text="Apply"
              intent={Intent.PRIMARY}
              onClick={() => {
                if (!outputName) {
                  AppToaster.show({
                    message: 'Must have a name',
                    intent: Intent.DANGER,
                  });
                  return;
                }

                if (
                  initExpression?.getOutputName() !== outputName &&
                  querySource.nameInUse(outputName)
                ) {
                  const availableName = querySource.getAvailableName(outputName);
                  AppToaster.show({
                    message: (
                      <>
                        The name <Tag minimal>{outputName}</Tag> is already in use.
                      </>
                    ),
                    intent: Intent.WARNING,
                    action: {
                      text: (
                        <>
                          Change to <Tag minimal>{availableName}</Tag>
                        </>
                      ),
                      onClick: () => setOutputName(availableName),
                    },
                  });
                  return;
                }

                let newExpression: SqlExpression;
                try {
                  newExpression = SqlExpression.parse(formula).as(outputName);
                } catch (e) {
                  AppToaster.show({
                    message: e.message,
                    intent: Intent.DANGER,
                  });
                  return;
                }

                if (initExpression) {
                  const initExpressionName = initExpression.getOutputName()!;
                  if (columnToDuplicate) {
                    onApply(
                      querySource.addColumnAfter(columnToDuplicate, newExpression),
                      undefined,
                    );
                  } else {
                    onApply(
                      querySource.changeColumn(initExpressionName, newExpression),
                      new Map([[initExpressionName, newExpression.getOutputName()!]]),
                    );
                  }
                } else {
                  onApply(querySource.addColumn(newExpression), undefined);
                }

                onClose();
              }}
            />
          </div>
        </div>
      </div>
    </Dialog>
  );
});
