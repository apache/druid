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
import * as JSONBig from 'json-bigint-native';
import React from 'react';

import { useQueryManager } from '../../hooks';
import { Api } from '../../singletons';
import { Loader } from '../loader/loader';
import { ShowValue } from '../show-value/show-value';

import './show-history.scss';

export interface VersionSpec {
  version: string;
  spec: any;
}

export interface ShowHistoryProps {
  endpoint: string;
  downloadFilename?: string;
}

export const ShowHistory = React.memo(function ShowHistory(props: ShowHistoryProps) {
  const { downloadFilename, endpoint } = props;

  const [historyState] = useQueryManager<string, VersionSpec[]>({
    processQuery: async (endpoint: string) => {
      const resp = await Api.instance.get(endpoint);
      return resp.data;
    },
    initQuery: endpoint,
  });

  if (historyState.loading) return <Loader />;
  if (!historyState.data) return null;

  const versions = historyState.data.map((pastSupervisor: VersionSpec, index: number) => (
    <Tab
      id={index}
      key={index}
      title={pastSupervisor.version}
      panel={
        <ShowValue
          jsonValue={
            pastSupervisor.spec
              ? JSONBig.stringify(pastSupervisor.spec, undefined, 2)
              : historyState.getErrorMessage()
          }
          downloadFilename={`version-${pastSupervisor.version}-${downloadFilename}`}
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
});
