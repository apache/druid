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

import { Intent, Label, ProgressBar } from '@blueprintjs/core';
import React, { useState } from 'react';

import { Execution } from '../../../druid-models';
import { CancelQueryDialog } from '../cancel-query-dialog/cancel-query-dialog';

import './execution-progress-bar-pane.scss';

export interface ExecutionProgressBarPaneProps {
  execution: Execution | undefined;
  onCancel?(): void;
  onToggleLiveReports?(): void;
  showLiveReports?: boolean;
}

export const ExecutionProgressBarPane = React.memo(function ExecutionProgressBarPane(
  props: ExecutionProgressBarPaneProps,
) {
  const { execution, onCancel, onToggleLiveReports, showLiveReports } = props;
  const [showCancelConfirm, setShowCancelConfirm] = useState(false);

  const stages = execution?.stages;

  function cancelMaybeConfirm() {
    if (!onCancel) return;
    if (execution?.isProcessingData()) {
      setShowCancelConfirm(true);
    } else {
      onCancel();
    }
  }

  const idx = stages ? stages.currentStageIndex() : -1;
  return (
    <div className="execution-progress-bar-pane">
      <Label>
        {stages
          ? execution.isWaitingForQuery()
            ? 'Running query...'
            : 'Query complete, waiting for segments to be loaded...'
          : 'Loading...'}
        {onCancel && (
          <>
            {' '}
            <span className="cancel" onClick={cancelMaybeConfirm}>
              {stages && !execution.isWaitingForQuery() ? '(stop waiting)' : '(cancel)'}
            </span>
          </>
        )}
      </Label>
      <ProgressBar
        className="overall"
        key={stages ? 'actual' : 'pending'}
        intent={stages ? Intent.PRIMARY : undefined}
        value={stages && execution.isWaitingForQuery() ? stages.overallProgress() : undefined}
      />
      {stages && idx >= 0 && (
        <>
          <Label>{`Current stage (${idx + 1} of ${stages.stageCount()})`}</Label>
          <ProgressBar
            className="stage"
            stripes={false}
            value={stages.stageProgress(stages.getStage(idx))}
          />
          {onToggleLiveReports && (
            <Label className="toggle-live-reports" onClick={onToggleLiveReports}>
              {showLiveReports ? 'Hide live reports' : 'Show live reports'}
            </Label>
          )}
        </>
      )}
      {showCancelConfirm && onCancel && (
        <CancelQueryDialog onCancel={onCancel} onDismiss={() => setShowCancelConfirm(false)} />
      )}
    </div>
  );
});
