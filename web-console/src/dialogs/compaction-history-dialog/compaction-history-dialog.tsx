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

import { Button, Callout, Classes, Code, Dialog, Tab, Tabs } from '@blueprintjs/core';
import * as JSONBig from 'json-bigint-native';
import React, { useState } from 'react';

import { Loader, ShowValue } from '../../components';
import type { CompactionConfig } from '../../druid-models';
import { useQueryManager } from '../../hooks';
import { Api } from '../../singletons';
import { formatInteger, formatPercent } from '../../utils';
import { DiffDialog } from '../diff-dialog/diff-dialog';

import './compaction-history-dialog.scss';

interface CompactionHistoryEntry {
  auditTime: string;
  auditInfo: any;
  globalConfig?: GlobalConfig;
  compactionConfig: CompactionConfig;
}

interface GlobalConfig {
  compactionTaskSlotRatio: number;
  maxCompactionTaskSlots: number;
  useAutoScaleSlots: boolean;
}

function formatGlobalConfig(globalConfig: GlobalConfig): string {
  return [
    `compactionTaskSlotRatio: ${formatPercent(globalConfig.compactionTaskSlotRatio)}`,
    `maxCompactionTaskSlots: ${formatInteger(globalConfig.maxCompactionTaskSlots)}`,
    `useAutoScaleSlots: ${globalConfig.useAutoScaleSlots}`,
  ].join('\n');
}

export interface CompactionHistoryDialogProps {
  datasource: string;
  onClose(): void;
}

export const CompactionHistoryDialog = React.memo(function CompactionHistoryDialog(
  props: CompactionHistoryDialogProps,
) {
  const { datasource, onClose } = props;

  const [diffIndex, setDiffIndex] = useState(-1);
  const [historyState] = useQueryManager<string, CompactionHistoryEntry[]>({
    initQuery: datasource,
    processQuery: async datasource => {
      try {
        const resp = await Api.instance.get(
          `/druid/coordinator/v1/config/compaction/${Api.encodePath(datasource)}/history?count=20`,
        );
        return resp.data;
      } catch (e) {
        if (e.response?.status === 404) return [];
        throw e;
      }
    },
  });

  const historyData = historyState.data;
  return (
    <Dialog
      className="compaction-history-dialog"
      isOpen
      onClose={onClose}
      canOutsideClickClose={false}
      title={`Compaction history: ${datasource}`}
    >
      <div className={Classes.DIALOG_BODY}>
        {historyData ? (
          historyData.length ? (
            <Tabs animate renderActiveTabPanelOnly vertical defaultSelectedTabId={0}>
              {historyData.map((historyEntry, i) => (
                <Tab
                  id={i}
                  key={i}
                  title={historyEntry.auditTime}
                  panelClassName="panel"
                  panel={
                    <>
                      <ShowValue
                        jsonValue={JSONBig.stringify(historyEntry.compactionConfig, undefined, 2)}
                        onDiffWithPrevious={
                          i < historyData.length - 1 ? () => setDiffIndex(i) : undefined
                        }
                        downloadFilename={`compaction-history-${datasource}-version-${historyEntry.auditTime}.json`}
                      />
                      {historyEntry.globalConfig && (
                        <Callout className="global-info">
                          {formatGlobalConfig(historyEntry.globalConfig)}
                        </Callout>
                      )}
                    </>
                  }
                />
              ))}
              <Tabs.Expander />
            </Tabs>
          ) : (
            <div>
              There is no compaction history for <Code>{datasource}</Code>.
            </div>
          )
        ) : historyState.loading ? (
          <Loader />
        ) : (
          <div>{historyState.getErrorMessage()}</div>
        )}
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
        </div>
      </div>
      {diffIndex !== -1 && historyData && (
        <DiffDialog
          title="Compaction config diff"
          versions={historyData.map(s => ({ label: s.auditTime, value: s.compactionConfig }))}
          initLeftIndex={diffIndex + 1}
          initRightIndex={diffIndex}
          onClose={() => setDiffIndex(-1)}
        />
      )}
    </Dialog>
  );
});
