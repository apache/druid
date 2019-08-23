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
import React from 'react';

import { CenterMessage } from '../../components';

import './query-history-dialog.scss';

export interface QueryRecord {
  version: string;
  queryString: string;
}
export interface QueryHistoryDialogProps {
  setQueryString: (queryString: string) => void;
  onClose: () => void;
  queryRecords: readonly QueryRecord[];
}

export interface QueryHistoryDialogState {
  activeTab: number;
}

export class QueryHistoryDialog extends React.PureComponent<
  QueryHistoryDialogProps,
  QueryHistoryDialogState
> {
  static getHistoryVersion(): string {
    return new Date()
      .toISOString()
      .split('.')[0]
      .replace('T', ' ');
  }

  static addQueryToHistory(
    queryHistory: readonly QueryRecord[],
    queryString: string,
  ): readonly QueryRecord[] {
    // Do not add to history if already the same as the last element
    if (queryHistory.length && queryHistory[0].queryString === queryString) {
      return queryHistory;
    }

    return [
      {
        version: QueryHistoryDialog.getHistoryVersion(),
        queryString,
      },
    ]
      .concat(queryHistory)
      .slice(0, 10);
  }

  constructor(props: QueryHistoryDialogProps) {
    super(props);
    this.state = {
      activeTab: 0,
    };
  }

  private handleSelect = () => {
    const { queryRecords, setQueryString } = this.props;
    const { activeTab } = this.state;

    setQueryString(queryRecords[activeTab].queryString);
  };

  private handleTabChange = (tab: number) => {
    this.setState({ activeTab: tab });
  };

  renderContent(): JSX.Element {
    const { queryRecords } = this.props;
    const { activeTab } = this.state;

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
        onChange={this.handleTabChange}
      >
        {versions}
        <Tabs.Expander />
      </Tabs>
    );
  }

  render(): JSX.Element {
    const { onClose, queryRecords } = this.props;

    return (
      <Dialog className="query-history-dialog" isOpen onClose={onClose} title="Query history">
        <div className={Classes.DIALOG_BODY}>{this.renderContent()}</div>
        <div className={Classes.DIALOG_FOOTER}>
          <div className={Classes.DIALOG_FOOTER_ACTIONS}>
            <Button text="Close" onClick={onClose} />
            {Boolean(queryRecords.length) && (
              <Button text="Open" intent={Intent.PRIMARY} onClick={this.handleSelect} />
            )}
          </div>
        </div>
      </Dialog>
    );
  }
}
