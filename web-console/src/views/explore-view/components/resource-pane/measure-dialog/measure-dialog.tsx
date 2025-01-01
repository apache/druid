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
import {
  L,
  type QueryResult,
  sql,
  SqlExpression,
  SqlQuery,
  SqlWithPart,
} from 'druid-query-toolkit';
import React, { useMemo, useState } from 'react';

import { AppToaster } from '../../../../../singletons';
import { Measure, QuerySource } from '../../../models';
import type { Rename } from '../../../utils';
import { PreviewPane } from '../../preview-pane/preview-pane';
import { SqlInput } from '../../sql-input/sql-input';

import './measure-dialog.scss';

export interface MeasureDialogProps {
  initMeasure: Measure | undefined;
  measureToDuplicate?: string;
  onApply(newQuery: SqlQuery, rename: Rename | undefined): void;
  querySource: QuerySource;
  runSqlQuery(query: string | SqlQuery): Promise<QueryResult>;
  onClose(): void;
}

export const MeasureDialog = React.memo(function MeasureDialog(props: MeasureDialogProps) {
  const { initMeasure, measureToDuplicate, onApply, querySource, runSqlQuery, onClose } = props;

  const [outputName, setOutputName] = useState(initMeasure?.name || '');
  const [formula, setFormula] = useState(String(initMeasure?.expression || ''));

  const previewQuery = useMemo(() => {
    const expression = SqlExpression.maybeParse(formula);
    if (!expression) return;
    return SqlQuery.from('t')
      .changeWithParts([SqlWithPart.simple('t', QuerySource.stripToBaseSource(querySource.query))])
      .addSelect(L('Overall').as('label'))
      .addSelect(expression.as('value'))
      .applyIf(querySource.hasBaseTimeColumn(), q =>
        q.addWhere(sql`MAX_DATA_TIME() - INTERVAL '14' DAY <= __time`),
      )
      .toString();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [querySource.query, formula]);

  return (
    <Dialog
      className="measure-dialog"
      isOpen
      onClose={onClose}
      title={initMeasure ? 'Edit measure' : 'Add measure'}
    >
      <div className={Classes.DIALOG_BODY}>
        <div className="controls">
          <FormGroup label="Name">
            <InputGroup
              value={outputName}
              onChange={e => {
                setOutputName(e.target.value.slice(0, Measure.MAX_NAME_LENGTH));
              }}
              placeholder="Measure name"
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

                if (initMeasure?.name !== outputName && querySource.nameInUse(outputName)) {
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
                  newExpression = SqlExpression.parse(formula);
                } catch (e) {
                  AppToaster.show({
                    message: e.message,
                    intent: Intent.DANGER,
                  });
                  return;
                }

                const newMeasure: Measure = new Measure({
                  as: outputName,
                  expression: newExpression,
                });

                if (initMeasure) {
                  const initMeasureName = initMeasure.name;
                  if (measureToDuplicate) {
                    onApply(querySource.addMeasureAfter(measureToDuplicate, newMeasure), undefined);
                  } else {
                    onApply(
                      querySource.changeMeasure(initMeasureName, newMeasure),
                      new Map([[initMeasureName, newExpression.getOutputName()!]]),
                    );
                  }
                } else {
                  onApply(querySource.addMeasure(newMeasure), undefined);
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
