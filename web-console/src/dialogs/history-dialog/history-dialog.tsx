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

import { Button, Classes, Dialog, Tab, Tabs } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import classNames from 'classnames';
import * as JSONBig from 'json-bigint-native';
import React, { useState } from 'react';

import { ShowValue } from '../../components';
import { DiffDialog } from '../diff-dialog/diff-dialog';

import './history-dialog.scss';

function normalizePayload(payload: string): string {
  try {
    return JSONBig.stringify(JSONBig.parse(payload), undefined, 2);
  } catch {
    return payload;
  }
}

interface HistoryRecord {
  auditInfo: {
    author?: string;
    comment?: string;
    ip?: string;
  };
  auditTime: string;
  payload: string;
}

interface HistoryDialogProps {
  className?: string;
  title: string;
  historyRecords: HistoryRecord[];
  onBack(): void;
}

export const HistoryDialog = React.memo(function HistoryDialog(props: HistoryDialogProps) {
  const { className, title, historyRecords, onBack } = props;

  const [diffIndex, setDiffIndex] = useState(-1);

  let content: JSX.Element;
  if (historyRecords.length === 0) {
    content = <div className="no-record">No history records available</div>;
  } else {
    content = (
      <Tabs animate renderActiveTabPanelOnly vertical defaultSelectedTabId={0}>
        {historyRecords.map(({ auditInfo, auditTime, payload }, i) => {
          const formattedTime = auditTime.replace('T', ' ').substring(0, auditTime.length - 5);
          return (
            <Tab
              id={i}
              key={i}
              title={`${auditInfo.comment || 'Change'} @ ${formattedTime}`}
              panel={
                <ShowValue
                  jsonValue={normalizePayload(payload)}
                  onDiffWithPrevious={
                    i < historyRecords.length - 1 ? () => setDiffIndex(i) : undefined
                  }
                />
              }
              panelClassName="panel"
            />
          );
        })}
        <Tabs.Expander />
      </Tabs>
    );
  }

  if (diffIndex !== -1) {
    return (
      <DiffDialog
        title="Supervisor spec diff"
        versions={historyRecords.map(({ auditInfo, auditTime, payload }) => ({
          label: auditInfo.comment || auditTime,
          value: normalizePayload(payload),
        }))}
        initLeftIndex={diffIndex + 1}
        initRightIndex={diffIndex}
        onClose={() => setDiffIndex(-1)}
      />
    );
  }

  return (
    <Dialog
      className={classNames('history-dialog', className)}
      isOpen
      title={title}
      onClose={onBack}
    >
      <div className={Classes.DIALOG_BODY}>{content}</div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button onClick={onBack} icon={IconNames.ARROW_LEFT} text="Back" />
        </div>
      </div>
    </Dialog>
  );
});
