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
import { Tooltip2 } from '@blueprintjs/popover2';
import React, { useState } from 'react';

import { StatusDialog } from '../../../dialogs/status-dialog/status-dialog';
import type { Capabilities } from '../../../helpers';
import { useQueryManager } from '../../../hooks';
import { Api } from '../../../singletons';
import type { NullModeDetection } from '../../../utils';
import {
  deepGet,
  explainNullModeDetection,
  NULL_DETECTION_QUERY,
  nullDetectionQueryResultDecoder,
  pluralIfNeeded,
  queryDruidRune,
  summarizeNullModeDetection,
} from '../../../utils';
import { HomeViewCard } from '../home-view-card/home-view-card';

interface StatusSummary {
  version: string;
  extensionCount: number;
}

export interface StatusCardProps {
  capabilities: Capabilities;
}

export const StatusCard = React.memo(function StatusCard(props: StatusCardProps) {
  const { capabilities } = props;
  const [showStatusDialog, setShowStatusDialog] = useState(false);
  const [statusSummaryState] = useQueryManager<null, StatusSummary>({
    initQuery: null,
    processQuery: async () => {
      const statusResp = await Api.instance.get('/status');
      return {
        version: statusResp.data.version,
        extensionCount: statusResp.data.modules.length,
      };
    },
  });

  const [nullModeDetectionState] = useQueryManager<Capabilities, NullModeDetection>({
    initQuery: capabilities,
    processQuery: async capabilities => {
      if (!capabilities.hasQuerying()) return {};
      const nullDetectionResponse = await queryDruidRune(NULL_DETECTION_QUERY);
      return nullDetectionQueryResultDecoder(deepGet(nullDetectionResponse, '0.result'));
    },
  });

  const statusSummary = statusSummaryState.data;
  const nullModeDetection = nullModeDetectionState.data;
  return (
    <>
      <HomeViewCard
        className="status-card"
        onClick={() => {
          setShowStatusDialog(true);
        }}
        icon={IconNames.GRAPH}
        title="Status"
        loading={statusSummaryState.loading}
        error={statusSummaryState.error}
      >
        {statusSummary && (
          <>
            <p>{`Apache Druid is running version ${statusSummary.version}`}</p>
            <p>{`${pluralIfNeeded(statusSummary.extensionCount, 'extension')} loaded`}</p>
          </>
        )}
        {nullModeDetection && (
          <Tooltip2
            content={
              <div>
                <p>
                  <strong>Null related server properties</strong>
                </p>
                {explainNullModeDetection(nullModeDetection).map((line, i) => (
                  <p key={i}>{line}</p>
                ))}
              </div>
            }
          >
            <p>{summarizeNullModeDetection(nullModeDetection)}</p>
          </Tooltip2>
        )}
      </HomeViewCard>
      {showStatusDialog && (
        <StatusDialog
          onClose={() => {
            setShowStatusDialog(false);
          }}
        />
      )}
    </>
  );
});
