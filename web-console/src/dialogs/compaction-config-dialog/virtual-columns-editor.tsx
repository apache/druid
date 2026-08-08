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

import { Button, Card, Classes, Collapse, Dialog, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { useState } from 'react';

import type { FormJsonTabs } from '../../components';
import { AutoForm, FormJsonSelector, JsonInput } from '../../components';
import type { ExpressionVirtualColumn } from '../../druid-models';
import { EXPRESSION_VIRTUAL_COLUMN_FIELDS, newExpressionVirtualColumn } from '../../druid-models';

import './virtual-columns-editor.scss';

export interface VirtualColumnsEditorProps {
  onClose: () => void;
  onSave: (virtualColumns: ExpressionVirtualColumn[]) => void;
  virtualColumns: ExpressionVirtualColumn[] | undefined;
}

export const VirtualColumnsEditor = React.memo(function VirtualColumnsEditor(
  props: VirtualColumnsEditorProps,
) {
  const { onClose, onSave, virtualColumns } = props;

  const [currentTab, setCurrentTab] = useState<FormJsonTabs>('form');
  const [currentColumns, setCurrentColumns] = useState<ExpressionVirtualColumn[]>(
    virtualColumns || [],
  );
  const [jsonError, setJsonError] = useState<Error | undefined>();
  const [openCards, setOpenCards] = useState<Record<number, boolean>>(
    Object.fromEntries((virtualColumns || []).map((_, i) => [i, true])),
  );

  function addColumn() {
    setCurrentColumns([...currentColumns, newExpressionVirtualColumn()]);
    setOpenCards({ ...openCards, [currentColumns.length]: true });
  }

  function deleteColumn(index: number) {
    setCurrentColumns(currentColumns.filter((_c, i) => i !== index));
  }

  function changeColumn(index: number, col: ExpressionVirtualColumn) {
    setCurrentColumns(currentColumns.map((c, i) => (i === index ? col : c)));
  }

  function toggleCard(index: number) {
    setOpenCards({ ...openCards, [index]: !openCards[index] });
  }

  return (
    <Dialog
      className="virtual-columns-editor"
      isOpen
      onClose={onClose}
      canOutsideClickClose={false}
      title="Edit virtual columns"
    >
      <FormJsonSelector
        tab={currentTab}
        onChange={t => {
          setJsonError(undefined);
          setCurrentTab(t);
        }}
      />
      <div className="content">
        {currentTab === 'form' ? (
          <div className="vc-list">
            {currentColumns.map((col, index) => (
              <Card key={index} className="vc-card" elevation={1}>
                <div className="vc-card-header">
                  <Button
                    minimal
                    small
                    icon={openCards[index] ? IconNames.CARET_DOWN : IconNames.CARET_RIGHT}
                    onClick={() => toggleCard(index)}
                  />
                  <span className="vc-card-name">{col.name || '(unnamed)'}</span>
                  <div className="spacer" />
                  <Button
                    minimal
                    small
                    icon={IconNames.TRASH}
                    intent={Intent.DANGER}
                    onClick={() => deleteColumn(index)}
                  />
                </div>
                <Collapse isOpen={Boolean(openCards[index])}>
                  <div className="vc-card-form">
                    <AutoForm
                      fields={EXPRESSION_VIRTUAL_COLUMN_FIELDS}
                      model={col}
                      onChange={m => changeColumn(index, m as ExpressionVirtualColumn)}
                    />
                  </div>
                </Collapse>
              </Card>
            ))}
            <Button icon={IconNames.PLUS} minimal onClick={addColumn}>
              Add virtual column
            </Button>
          </div>
        ) : (
          <JsonInput
            value={currentColumns}
            onChange={v => setCurrentColumns(v)}
            setError={setJsonError}
            height="100%"
          />
        )}
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button
            text="Save"
            intent={Intent.PRIMARY}
            disabled={Boolean(jsonError)}
            onClick={() => {
              onSave(currentColumns.length ? currentColumns : []);
              onClose();
            }}
          />
        </div>
      </div>
    </Dialog>
  );
});
