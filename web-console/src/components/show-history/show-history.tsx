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
import React, { useState } from 'react';

import { DiffDialog } from '../../dialogs';
import { cleanSpec, IngestionSpec } from '../../druid-models';
import { useQueryManager } from '../../hooks';
import { Api } from '../../singletons';
import { deepSet } from '../../utils';
import { Loader } from '../loader/loader';
import { ShowValue } from '../show-value/show-value';

import './show-history.scss';

export interface VersionSpec {
  version: string;
  spec: IngestionSpec;
}

export interface ShowHistoryProps {
  endpoint: string;
  downloadFilenamePrefix?: string;
}

export const ShowHistory = React.memo(function ShowHistory(props: ShowHistoryProps) {
  const { downloadFilenamePrefix, endpoint } = props;

  const [historyState] = useQueryManager<string, VersionSpec[]>({
    processQuery: async (endpoint: string) => {
      const resp = await Api.instance.get(endpoint);
      return resp.data.map((vs: VersionSpec) => deepSet(vs, 'spec', cleanSpec(vs.spec, true)));
    },
    initQuery: endpoint,
  });
  const [diffIndex, setDiffIndex] = useState(-1);

  if (historyState.loading) return <Loader />;

  const historyData = historyState.data;
  if (!historyData) return null;

  return (
    <div className="show-history">
      <Tabs animate renderActiveTabPanelOnly vertical defaultSelectedTabId={0}>
        {historyData.map((pastSupervisor, i) => (
          <Tab
            id={i}
            key={i}
            title={pastSupervisor.version}
            panel={
              <ShowValue
                jsonValue={JSONBig.stringify(pastSupervisor.spec, undefined, 2)}
                onDiffWithPrevious={i < historyData.length - 1 ? () => setDiffIndex(i) : undefined}
                downloadFilename={`${downloadFilenamePrefix}-version-${pastSupervisor.version}.json`}
              />
            }
            panelClassName="panel"
          />
        ))}
        <Tabs.Expander />
      </Tabs>
      {diffIndex !== -1 && (
        <DiffDialog
          title="Supervisor spec diff"
          versions={historyData.map(s => ({ label: s.version, value: s.spec }))}
          initLeftIndex={diffIndex + 1}
          initRightIndex={diffIndex}
          onClose={() => setDiffIndex(-1)}
        />
      )}
    </div>
  );
});
