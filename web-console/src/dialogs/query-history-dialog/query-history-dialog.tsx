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

import { Button, Classes, Dialog, Intent, Tab, Tabs, TextArea } from '@blueprintjs/core';
import React, { useState } from 'react';

import { CenterMessage } from '../../components';
import { QueryRecord } from '../../utils/query-history';

import './query-history-dialog.scss';

export interface QueryHistoryDialogProps {
  queryRecords: readonly QueryRecord[];
  setQueryString: (queryString: string, queryContext: Record<string, any>) => void;
  onClose: () => void;
}

export const QueryHistoryDialog = React.memo(function QueryHistoryDialog(
  props: QueryHistoryDialogProps,
) {
  const [activeTab, setActiveTab] = useState(0);
  const { queryRecords, setQueryString, onClose } = props;

  function handleSelect() {
    const queryRecord = queryRecords[activeTab];
    setQueryString(queryRecord.queryString, queryRecord.queryContext || {});
    onClose();
  }

  function renderContent(): JSX.Element {
    if (!queryRecords.length) {
      return <CenterMessage>The query history is empty.</CenterMessage>;
    }

    const versions = queryRecords.map((record, index) => (
      <Tab
        id={index}
        key={index}
        title={record.version}
        panel={<TextArea readOnly value={record.queryString} className={'text-area'} />}
        panelClassName={'panel'}
      />
    ));

    return (
      <Tabs
        animate
        renderActiveTabPanelOnly
        vertical
        className={'tab-area'}
        selectedTabId={activeTab}
        onChange={(t: number) => setActiveTab(t)}
      >
        {versions}
        <Tabs.Expander />
      </Tabs>
    );
  }

  return (
    <Dialog className="query-history-dialog" isOpen onClose={onClose} title="Query history">
      <div className={Classes.DIALOG_BODY}>{renderContent()}</div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          {Boolean(queryRecords.length) && (
            <Button text="Open" intent={Intent.PRIMARY} onClick={handleSelect} />
          )}
        </div>
      </div>
    </Dialog>
  );
});
