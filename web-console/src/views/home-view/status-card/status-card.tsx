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
import { pluralIfNeeded, QueryManager } from '../../../utils';
import { HomeViewCard } from '../home-view-card/home-view-card';

export interface StatusCardProps {}

export interface StatusCardState {
  statusLoading: boolean;
  version?: string;
  extensionCount?: number;
  statusError?: string;

  showStatusDialog: boolean;
}

interface StatusSummary {
  version: string;
  extensionCount: number;
}

export class StatusCard extends React.PureComponent<StatusCardProps, StatusCardState> {
  private versionQueryManager: QueryManager<null, StatusSummary>;

  constructor(props: StatusCardProps, context: any) {
    super(props, context);

    this.state = {
      statusLoading: true,
      showStatusDialog: false,
    };

    this.versionQueryManager = new QueryManager({
      processQuery: async () => {
        const statusResp = await axios.get('/status');
        return {
          version: statusResp.data.version,
          extensionCount: statusResp.data.modules.length,
        };
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          statusLoading: loading,
          version: result ? result.version : undefined,
          extensionCount: result ? result.extensionCount : undefined,
          statusError: error,
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

  private handleStatusDialogOpen = () => {
    this.setState({ showStatusDialog: true });
  };

  private handleStatusDialogClose = () => {
    this.setState({ showStatusDialog: false });
  };

  renderStatusDialog() {
    const { showStatusDialog } = this.state;
    if (!showStatusDialog) return;

    return <StatusDialog onClose={this.handleStatusDialogClose} />;
  }

  render(): JSX.Element {
    const { version, extensionCount, statusLoading, statusError } = this.state;

    return (
      <>
        <HomeViewCard
          className="status-card"
          onClick={this.handleStatusDialogOpen}
          icon={IconNames.GRAPH}
          title="Status"
          loading={statusLoading}
          error={statusError}
        >
          {version && <p>{`Apache Druid is running version ${version}`}</p>}
          {extensionCount && <p>{`${pluralIfNeeded(extensionCount, 'extension')} loaded`}</p>}
        </HomeViewCard>
        {this.renderStatusDialog()}
      </>
    );
  }
}
