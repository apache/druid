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

import { Code, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import React from 'react';

import { AutoForm, ExternalLink } from '../../components';
import { AppToaster } from '../../singletons/toaster';
import { getDruidErrorMessage, QueryManager } from '../../utils';
import { SnitchDialog } from '../snitch-dialog/snitch-dialog';

import './coordinator-dynamic-config-dialog.scss';

export interface CoordinatorDynamicConfigDialogProps {
  onClose: () => void;
}

export interface CoordinatorDynamicConfigDialogState {
  dynamicConfig?: Record<string, any>;
  historyRecords: any[];
}

export class CoordinatorDynamicConfigDialog extends React.PureComponent<
  CoordinatorDynamicConfigDialogProps,
  CoordinatorDynamicConfigDialogState
> {
  private historyQueryManager: QueryManager<null, any>;

  constructor(props: CoordinatorDynamicConfigDialogProps) {
    super(props);
    this.state = {
      historyRecords: [],
    };

    this.historyQueryManager = new QueryManager({
      processQuery: async () => {
        const historyResp = await axios(`/druid/coordinator/v1/config/history?count=100`);
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
    this.getClusterConfig();

    this.historyQueryManager.runQuery(null);
  }

  async getClusterConfig() {
    let config: Record<string, any> | undefined;
    try {
      const configResp = await axios.get('/druid/coordinator/v1/config');
      config = configResp.data;
    } catch (e) {
      AppToaster.show({
        icon: IconNames.ERROR,
        intent: Intent.DANGER,
        message: `Could not load coordinator dynamic config: ${getDruidErrorMessage(e)}`,
      });
      return;
    }
    this.setState({
      dynamicConfig: config,
    });
  }

  private saveClusterConfig = async (comment: string) => {
    const { onClose } = this.props;
    const newState: any = this.state.dynamicConfig;
    try {
      await axios.post('/druid/coordinator/v1/config', newState, {
        headers: {
          'X-Druid-Author': 'console',
          'X-Druid-Comment': comment,
        },
      });
    } catch (e) {
      AppToaster.show({
        icon: IconNames.ERROR,
        intent: Intent.DANGER,
        message: `Could not save coordinator dynamic config: ${getDruidErrorMessage(e)}`,
      });
    }

    AppToaster.show({
      message: 'Saved coordinator dynamic config',
      intent: Intent.SUCCESS,
    });
    onClose();
  };

  render(): JSX.Element {
    const { onClose } = this.props;
    const { dynamicConfig, historyRecords } = this.state;

    return (
      <SnitchDialog
        className="coordinator-dynamic-config-dialog"
        onSave={this.saveClusterConfig}
        onClose={onClose}
        title="Coordinator dynamic config"
        historyRecords={historyRecords}
      >
        <p>
          Edit the coordinator dynamic configuration on the fly. For more information please refer
          to the{' '}
          <ExternalLink href="https://druid.apache.org/docs/latest/configuration/index.html#dynamic-configuration">
            documentation
          </ExternalLink>
          .
        </p>
        <AutoForm
          fields={[
            {
              name: 'maxSegmentsToMove',
              type: 'number',
              defaultValue: 5,
              info: <>The maximum number of segments that can be moved at any given time.</>,
            },
            {
              name: 'balancerComputeThreads',
              type: 'number',
              defaultValue: 1,
              info: (
                <>
                  Thread pool size for computing moving cost of segments in segment balancing.
                  Consider increasing this if you have a lot of segments and moving segments starts
                  to get stuck.
                </>
              ),
            },
            {
              name: 'emitBalancingStats',
              type: 'boolean',
              defaultValue: false,
              info: (
                <>
                  Boolean flag for whether or not we should emit balancing stats. This is an
                  expensive operation.
                </>
              ),
            },
            {
              name: 'killAllDataSources',
              type: 'boolean',
              defaultValue: false,
              info: (
                <>
                  Send kill tasks for ALL dataSources if property{' '}
                  <Code>druid.coordinator.kill.on</Code> is true. If this is set to true then{' '}
                  <Code>killDataSourceWhitelist</Code> must not be specified or be empty list.
                </>
              ),
            },
            {
              name: 'killDataSourceWhitelist',
              type: 'string-array',
              info: (
                <>
                  List of dataSources for which kill tasks are sent if property{' '}
                  <Code>druid.coordinator.kill.on</Code> is true. This can be a list of
                  comma-separated dataSources or a JSON array.
                </>
              ),
            },
            {
              name: 'killPendingSegmentsSkipList',
              type: 'string-array',
              info: (
                <>
                  List of dataSources for which pendingSegments are NOT cleaned up if property{' '}
                  <Code>druid.coordinator.kill.pendingSegments.on</Code> is true. This can be a list
                  of comma-separated dataSources or a JSON array.
                </>
              ),
            },
            {
              name: 'maxSegmentsInNodeLoadingQueue',
              type: 'number',
              defaultValue: 0,
              info: (
                <>
                  The maximum number of segments that could be queued for loading to any given
                  server. This parameter could be used to speed up segments loading process,
                  especially if there are "slow" nodes in the cluster (with low loading speed) or if
                  too much segments scheduled to be replicated to some particular node (faster
                  loading could be preferred to better segments distribution). Desired value depends
                  on segments loading speed, acceptable replication time and number of nodes. Value
                  1000 could be a start point for a rather big cluster. Default value is 0 (loading
                  queue is unbounded)
                </>
              ),
            },
            {
              name: 'mergeBytesLimit',
              type: 'size-bytes',
              defaultValue: 524288000,
              info: <>The maximum total uncompressed size in bytes of segments to merge.</>,
            },
            {
              name: 'mergeSegmentsLimit',
              type: 'number',
              defaultValue: 100,
              info: <>The maximum number of segments that can be in a single append task.</>,
            },
            {
              name: 'millisToWaitBeforeDeleting',
              type: 'number',
              defaultValue: 900000,
              info: (
                <>
                  How long does the Coordinator need to be active before it can start removing
                  (marking unused) segments in metadata storage.
                </>
              ),
            },
            {
              name: 'replicantLifetime',
              type: 'number',
              defaultValue: 15,
              info: (
                <>
                  The maximum number of Coordinator runs for a segment to be replicated before we
                  start alerting.
                </>
              ),
            },
            {
              name: 'replicationThrottleLimit',
              type: 'number',
              defaultValue: 10,
              info: <>The maximum number of segments that can be replicated at one time.</>,
            },
            {
              name: 'decommissioningNodes',
              type: 'string-array',
              info: (
                <>
                  List of historical servers to 'decommission'. Coordinator will not assign new
                  segments to 'decommissioning' servers, and segments will be moved away from them
                  to be placed on non-decommissioning servers at the maximum rate specified by{' '}
                  <Code>decommissioningMaxPercentOfMaxSegmentsToMove</Code>.
                </>
              ),
            },
            {
              name: 'decommissioningMaxPercentOfMaxSegmentsToMove',
              type: 'number',
              defaultValue: 70,
              info: (
                <>
                  The maximum number of segments that may be moved away from 'decommissioning'
                  servers to non-decommissioning (that is, active) servers during one Coordinator
                  run. This value is relative to the total maximum segment movements allowed during
                  one run which is determined by <Code>maxSegmentsToMove</Code>. If
                  <Code>decommissioningMaxPercentOfMaxSegmentsToMove</Code> is 0, segments will
                  neither be moved from or to 'decommissioning' servers, effectively putting them in
                  a sort of "maintenance" mode that will not participate in balancing or assignment
                  by load rules. Decommissioning can also become stalled if there are no available
                  active servers to place the segments. By leveraging the maximum percent of
                  decommissioning segment movements, an operator can prevent active servers from
                  overload by prioritizing balancing, or decrease decommissioning time instead. The
                  value should be between 0 and 100.
                </>
              ),
            },
          ]}
          model={dynamicConfig}
          onChange={m => this.setState({ dynamicConfig: m })}
        />
      </SnitchDialog>
    );
  }
}
