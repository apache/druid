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
import React, { useState } from 'react';

import { StatusDialog } from '../../../dialogs/status-dialog/status-dialog';
import { useQueryManager } from '../../../hooks';
import { pluralIfNeeded } from '../../../utils';
import { HomeViewCard } from '../home-view-card/home-view-card';

interface StatusSummary {
  version: string;
  extensionCount: number;
}

export interface StatusCardProps {}

export const StatusCard = React.memo(function StatusCard(_props: StatusCardProps) {
  const [showStatusDialog, setShowStatusDialog] = useState(false);
  const [statusSummaryState] = useQueryManager<null, StatusSummary>({
    processQuery: async () => {
      const statusResp = await axios.get('/status');
      return {
        version: statusResp.data.version,
        extensionCount: statusResp.data.modules.length,
      };
    },
    initQuery: null,
  });

  const statusSummary = statusSummaryState.data;
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
