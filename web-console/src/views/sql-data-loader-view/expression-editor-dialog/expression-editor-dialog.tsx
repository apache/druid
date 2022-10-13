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

import { Button, Classes, Dialog, FormGroup, InputGroup, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { SqlExpression } from 'druid-query-toolkit';
import React, { useState } from 'react';

import { FlexibleQueryInput } from '../../workbench-view/flexible-query-input/flexible-query-input';

import './expression-editor-dialog.scss';

interface ExpressionEditorDialogProps {
  title?: string;
  includeOutputName?: boolean;
  expression?: SqlExpression;
  onSave(expression: SqlExpression): void;
  onDelete?(): void;
  onClose(): void;
}

export const ExpressionEditorDialog = React.memo(function ExpressionEditorDialog(
  props: ExpressionEditorDialogProps,
) {
  const { title, includeOutputName, expression, onSave, onDelete, onClose } = props;

  const [outputName, setOutputName] = useState<string>(() => expression?.getOutputName() || '');
  const [formula, setFormula] = useState<string>(
    () => expression?.getUnderlyingExpression()?.toString() || '',
  );

  const parsedExpression = formula ? SqlExpression.maybeParse(formula) : undefined;

  return (
    <Dialog
      className="expression-editor-dialog"
      onClose={onClose}
      isOpen
      title={title || 'Edit expression'}
      canOutsideClickClose={false}
    >
      <div className={Classes.DIALOG_BODY}>
        <FormGroup>
          <FlexibleQueryInput
            autoHeight={false}
            showGutter={false}
            placeholder="expression"
            queryString={formula}
            onQueryStringChange={setFormula}
            columnMetadata={undefined}
          />
        </FormGroup>
        {includeOutputName && (
          <FormGroup label="Output name">
            <InputGroup value={outputName} onChange={e => setOutputName(e.target.value)} />
          </FormGroup>
        )}
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          {onDelete && (
            <Button
              className="delete-button"
              icon={IconNames.TRASH}
              text="Delete"
              intent={Intent.DANGER}
              onClick={() => {
                onDelete();
                onClose();
              }}
            />
          )}
          <Button text="Close" onClick={onClose} />
          <Button
            text="Save"
            intent={Intent.PRIMARY}
            disabled={!parsedExpression}
            onClick={() => {
              if (!parsedExpression) return;
              let newExpression = parsedExpression;
              if (includeOutputName && newExpression.getOutputName() !== outputName) {
                newExpression = newExpression.as(outputName);
              }
              onSave(newExpression);
              onClose();
            }}
          />
        </div>
      </div>
    </Dialog>
  );
});
