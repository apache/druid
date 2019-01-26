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

import * as React from 'react';
import axios from 'axios';

import { AutoForm } from '../components/auto-form';

import { SnitchDialog } from './snitch-dialog';

import './coordinator-dynamic-config.scss';


export interface CoordinatorDynamicConfigDialogProps extends React.Props<any> {
  isOpen: boolean,
  onClose: () => void
}

export interface CoordinatorDynamicConfigDialogState {
  config: Record<string, any> | null;
}

export class CoordinatorDynamicConfigDialog extends React.Component<CoordinatorDynamicConfigDialogProps, CoordinatorDynamicConfigDialogState> {
  constructor(props: CoordinatorDynamicConfigDialogProps) {
    super(props);
    this.state = {
      config: null
    }
  }

  async getClusterConfig() {
    let config: Record<string, any> | null = null;
    try {
      const configResp = await axios.get("/druid/coordinator/v1/config");
      config = configResp.data
    } catch (error) {
      console.error(error)
    }
    this.setState({
      config
    });
  }

  saveClusterConfig = (author: string, comment: string) => {
    const { onClose } = this.props;
    let newState: any = this.state.config;
    const whiteList: any[] = newState["killDataSourceWhitelist"];
    const skipList: any[] = newState["killPendingSegmentsSkipList"];
    //newState["killDataSourceWhitelist"] = newState["killDataSourceWhitelist"].join(",");
    //newState["killPendingSegmentsSkipList"] = newState["killPendingSegmentsSkipList"].join(",");
    axios.post("/druid/coordinator/v1/config", newState, {
      headers:{
        "X-Druid-Author": author,
        "X-Druid-Comment": comment
      }
    });
    newState["killDataSourceWhitelist"] = whiteList;
    newState["killPendingSegmentsSkipList"] = skipList;
    onClose();
  }

  render() {
    const { isOpen, onClose } = this.props;
    const { config } = this.state;

    return <SnitchDialog
      className="coordinator-dynamic-config"
      isOpen={ isOpen }
      onSave={this.saveClusterConfig}
      onOpening={() => {this.getClusterConfig()}}
      onClose={ onClose }
      title="Coordinator dynamic config"
    >
      <p>
        Edit the coordinator dynamic configuration on the fly.
        For more information please refer to the <a href="http://druid.io/docs/latest/configuration/index.html#dynamic-configuration" target="_blank">documentation</a>.
      </p>
      <AutoForm
        fields={[
          {
            name: "balancerComputeThreads",
            type: "number"
          },
          {
            name: "emitBalancingStats",
            type: "boolean"
          },
          {
            name: "killAllDataSources",
            type: "boolean"
          },
          {
            name: "killDataSourceWhitelist",
            type: "string-array"
          },
          {
            name: "killPendingSegmentsSkipList",
            type: "string-array"
          },
          {
            name: "maxSegmentsInNodeLoadingQueue",
            type: "number"
          },
          {
            name: "maxSegmentsToMove",
            type: "number"
          },
          {
            name: "mergeBytesLimit",
            type: "size-bytes"
          },
          {
            name: "mergeSegmentsLimit",
            type: "number"
          },
          {
            name: "millisToWaitBeforeDeleting",
            type: "number"
          },
          {
            name: "replicantLifetime",
            type: "number"
          },
          {
            name: "replicationThrottleLimit",
            type: "number"
          }
        ]}
        model={config}
        onChange={m => this.setState({ config: m })}
      />
    </SnitchDialog>
  }
}
