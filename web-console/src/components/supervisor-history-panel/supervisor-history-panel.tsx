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
import type { IngestionSpec } from '../../druid-models';
import { cleanSpec } from '../../druid-models';
import { useQueryManager } from '../../hooks';
import { Api } from '../../singletons';
import { deepSet } from '../../utils';
import { Loader } from '../loader/loader';
import { ShowValue } from '../show-value/show-value';

import './supervisor-history-panel.scss';

export interface SupervisorHistoryEntry {
  version: string;
  spec: IngestionSpec;
}

export interface SupervisorHistoryPanelProps {
  supervisorId: string;
}

export const SupervisorHistoryPanel = React.memo(function SupervisorHistoryPanel(
  props: SupervisorHistoryPanelProps,
) {
  const { supervisorId } = props;

  const [diffIndex, setDiffIndex] = useState(-1);
  const [historyState] = useQueryManager<string, SupervisorHistoryEntry[]>({
    initQuery: supervisorId,
    processQuery: async supervisorId => {
      const resp = await Api.instance.get(
        `/druid/indexer/v1/supervisor/${Api.encodePath(supervisorId)}/history`,
      );
      return resp.data.map((vs: SupervisorHistoryEntry) =>
        deepSet(vs, 'spec', cleanSpec(vs.spec, true)),
      );
    },
  });

  if (historyState.loading) return <Loader />;

  const historyData = historyState.data;
  if (!historyData) return null;

  return (
    <div className="supervisor-history-panel">
      <Tabs animate renderActiveTabPanelOnly vertical defaultSelectedTabId={0}>
        {historyData.map((pastSupervisor, i) => (
          <Tab
            id={i}
            key={i}
            title={pastSupervisor.version}
            panelClassName="panel"
            panel={
              <ShowValue
                jsonValue={JSONBig.stringify(pastSupervisor.spec, undefined, 2)}
                onDiffWithPrevious={i < historyData.length - 1 ? () => setDiffIndex(i) : undefined}
                downloadFilename={`supervisor-${supervisorId}-version-${pastSupervisor.version}.json`}
              />
            }
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
