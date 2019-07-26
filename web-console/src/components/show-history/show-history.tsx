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

import { Tab, Tabs } from '@blueprintjs/core';
import axios from 'axios';
import React from 'react';

import { ShowJson } from '..';

import './show-history.scss';

export interface ShowHistoryProps {
  endpoint: string;
  transform?: (x: any) => any;
  downloadFilename?: string;
}

export interface ShowHistoryState {
  jsonValue: [];
  versions: any;
}

export class ShowHistory extends React.PureComponent<ShowHistoryProps, ShowHistoryState> {
  constructor(props: ShowHistoryProps, context: any) {
    super(props, context);
    this.state = {
      versions: [],
      jsonValue: [],
    };

    this.getJsonInfo();
  }

  private getJsonInfo = async (): Promise<void> => {
    const { endpoint, transform, downloadFilename } = this.props;
    try {
      const resp = await axios.get(endpoint);
      const data = resp.data;
      this.setState({
        jsonValue: data,
        versions: data.map((version: any, index: number) => (
          <Tab
            id={index}
            key={index}
            title={version.version}
            panel={
              <ShowJson
                defaultValue={JSON.stringify(version.spec, undefined, 2)}
                transform={transform}
                downloadFilename={downloadFilename + 'version-' + version.version}
                endpoint={endpoint}
              />
            }
            panelClassName={'panel'}
          />
        )),
      });
    } catch (e) {
      this.setState({
        jsonValue: [],
      });
    }
  };

  render() {
    const { versions } = this.state;
    return (
      <div className="show-history">
        <Tabs
          animate
          renderActiveTabPanelOnly
          vertical
          className={'tab-area'}
          defaultSelectedTabId={0}
        >
          {versions}
          <Tabs.Expander />
        </Tabs>
      </div>
    );
  }
}
