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

import { QueryManager } from '../../utils';
import { Loader } from '../loader/loader';
import { ShowValue } from '../show-value/show-value';

import './show-history.scss';

export interface PastSupervisor {
  version: string;
  spec: any;
}
export interface ShowHistoryProps {
  endpoint: string;
  transform?: (x: any) => any;
  downloadFilename?: string;
}

export interface ShowHistoryState {
  data?: PastSupervisor[];
  loading: boolean;
  error?: string;
}

export class ShowHistory extends React.PureComponent<ShowHistoryProps, ShowHistoryState> {
  private showHistoryQueryManager: QueryManager<null, string>;
  constructor(props: ShowHistoryProps, context: any) {
    super(props, context);
    this.state = {
      data: [],
      loading: true,
    };

    this.showHistoryQueryManager = new QueryManager({
      processQuery: async () => {
        const resp = await axios.get(this.props.endpoint);
        return resp.data;
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          loading,
          data: result,
          error,
        });
      },
    });
  }

  componentDidMount(): void {
    this.showHistoryQueryManager.runQuery(null);
  }

  render() {
    const { downloadFilename, endpoint } = this.props;
    const { data, loading } = this.state;
    if (loading) return <Loader />;
    if (!data) return;
    const versions = data.map((pastSupervisor: PastSupervisor, index: number) => (
      <Tab
        id={index}
        key={index}
        title={pastSupervisor.version}
        panel={
          <ShowValue
            jsonValue={JSON.stringify(pastSupervisor.spec, undefined, 2)}
            downloadFilename={downloadFilename + 'version-' + pastSupervisor.version}
            endpoint={endpoint}
          />
        }
        panelClassName={'panel'}
      />
    ));

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
