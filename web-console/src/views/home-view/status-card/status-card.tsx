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

import { IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import React from 'react';

import { StatusDialog } from '../../../dialogs/status-dialog/status-dialog';
import { QueryManager } from '../../../utils';
import { HomeViewCard } from '../home-view-card/home-view-card';

export interface StatusCardProps {}

export interface StatusCardState {
  versionLoading: boolean;
  version: string;
  versionError?: string;

  showStatusDialog: boolean;
}

export class StatusCard extends React.PureComponent<StatusCardProps, StatusCardState> {
  private versionQueryManager: QueryManager<null, string>;

  constructor(props: StatusCardProps, context: any) {
    super(props, context);
    this.state = {
      versionLoading: true,
      version: '',

      showStatusDialog: false,
    };

    this.versionQueryManager = new QueryManager({
      processQuery: async () => {
        const statusResp = await axios.get('/status');
        return statusResp.data.version;
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          versionLoading: loading,
          version: result,
          versionError: error,
        });
      },
    });
  }

  componentDidMount(): void {
    this.versionQueryManager.runQuery(null);
  }

  componentWillUnmount(): void {
    this.versionQueryManager.terminate();
  }

  renderStatusDialog() {
    const { showStatusDialog } = this.state;
    if (!showStatusDialog) {
      return null;
    }
    return (
      <StatusDialog
        onClose={() => this.setState({ showStatusDialog: false })}
        title={'Status'}
        isOpen
      />
    );
  }

  render(): JSX.Element {
    const { version, versionLoading, versionError } = this.state;

    return (
      <HomeViewCard
        className="status-card"
        onClick={() => this.setState({ showStatusDialog: true })}
        icon={IconNames.GRAPH}
        title="Status"
        loading={versionLoading}
        error={versionError}
      >
        {version ? `Apache Druid is running version ${version}` : ''}
        {this.renderStatusDialog()}
      </HomeViewCard>
    );
  }
}
