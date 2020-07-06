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

import { Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import React from 'react';

import { SnitchDialog } from '..';
import { AutoForm, ExternalLink } from '../../components';
import { getLink } from '../../links';
import { AppToaster } from '../../singletons/toaster';
import { getDruidErrorMessage, QueryManager } from '../../utils';

import './overlord-dynamic-config-dialog.scss';

export interface OverlordDynamicConfigDialogProps {
  onClose: () => void;
}

export interface OverlordDynamicConfigDialogState {
  dynamicConfig?: Record<string, any>;
  historyRecords: any[];
}

export class OverlordDynamicConfigDialog extends React.PureComponent<
  OverlordDynamicConfigDialogProps,
  OverlordDynamicConfigDialogState
> {
  private historyQueryManager: QueryManager<string, any>;

  constructor(props: OverlordDynamicConfigDialogProps) {
    super(props);
    this.state = {
      historyRecords: [],
    };

    this.historyQueryManager = new QueryManager({
      processQuery: async () => {
        const historyResp = await axios(`/druid/indexer/v1/worker/history?count=100`);
        return historyResp.data;
      },
      onStateChange: ({ result }) => {
        this.setState({
          historyRecords: result,
        });
      },
    });
  }

  componentDidMount() {
    this.getConfig();

    this.historyQueryManager.runQuery(`dummy`);
  }

  async getConfig() {
    let config: Record<string, any> | undefined;
    try {
      const configResp = await axios.get('/druid/indexer/v1/worker');
      config = configResp.data || {};
    } catch (e) {
      AppToaster.show({
        icon: IconNames.ERROR,
        intent: Intent.DANGER,
        message: `Could not load overlord dynamic config: ${getDruidErrorMessage(e)}`,
      });
      return;
    }
    this.setState({
      dynamicConfig: config,
    });
  }

  private saveConfig = async (comment: string) => {
    const { onClose } = this.props;
    const newState: any = this.state.dynamicConfig;
    try {
      await axios.post('/druid/indexer/v1/worker', newState, {
        headers: {
          'X-Druid-Author': 'console',
          'X-Druid-Comment': comment,
        },
      });
    } catch (e) {
      AppToaster.show({
        icon: IconNames.ERROR,
        intent: Intent.DANGER,
        message: `Could not save overlord dynamic config: ${getDruidErrorMessage(e)}`,
      });
    }

    AppToaster.show({
      message: 'Saved overlord dynamic config',
      intent: Intent.SUCCESS,
    });
    onClose();
  };

  render(): JSX.Element {
    const { onClose } = this.props;
    const { dynamicConfig, historyRecords } = this.state;

    return (
      <SnitchDialog
        className="overlord-dynamic-config-dialog"
        onSave={this.saveConfig}
        onClose={onClose}
        title="Overlord dynamic config"
        historyRecords={historyRecords}
      >
        <p>
          Edit the overlord dynamic configuration on the fly. For more information please refer to
          the{' '}
          <ExternalLink
            href={`${getLink('DOCS')}/configuration/index.html#overlord-dynamic-configuration`}
          >
            documentation
          </ExternalLink>
          .
        </p>
        <AutoForm
          fields={[
            {
              name: 'selectStrategy',
              type: 'json',
            },
            {
              name: 'autoScaler',
              type: 'json',
            },
          ]}
          model={dynamicConfig}
          onChange={m => this.setState({ dynamicConfig: m })}
        />
      </SnitchDialog>
    );
  }
}
