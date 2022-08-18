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

import classNames from 'classnames';
import React, { useState } from 'react';

import { Execution } from '../../../druid-models';
import { ExecutionProgressBarPane } from '../execution-progress-bar-pane/execution-progress-bar-pane';
import { ExecutionStagesPane } from '../execution-stages-pane/execution-stages-pane';

import './execution-progress-pane.scss';

export interface ExecutionProgressPaneProps {
  execution: Execution;
  intermediateError?: Error;
  goToIngestion(taskId: string): void;
  onCancel?(): void;
  allowLiveReportsPane?: boolean;
}

export const ExecutionProgressPane = React.memo(function ExecutionProgressPane(
  props: ExecutionProgressPaneProps,
) {
  const { execution, intermediateError, onCancel, allowLiveReportsPane, goToIngestion } = props;
  const [showLiveReports, setShowLiveReports] = useState(true);

  return (
    <div className={classNames('execution-progress-pane', { stale: intermediateError })}>
      <ExecutionProgressBarPane
        execution={execution}
        onToggleLiveReports={
          allowLiveReportsPane ? () => setShowLiveReports(!showLiveReports) : undefined
        }
        showLiveReports={allowLiveReportsPane && showLiveReports}
        onCancel={onCancel}
      />
      {allowLiveReportsPane && showLiveReports && execution.stages && (
        <ExecutionStagesPane execution={execution} goToIngestion={goToIngestion} />
      )}
      {intermediateError && (
        <div className="network-error-notification">
          <div className="message">Network connectivity issues...</div>
        </div>
      )}
    </div>
  );
});
