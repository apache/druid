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

import { Button, ButtonGroup } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { useState } from 'react';

import { Execution } from '../../../druid-models';
import { useInterval } from '../../../hooks';
import { formatDurationHybrid } from '../../../utils';
import { CancelQueryDialog } from '../cancel-query-dialog/cancel-query-dialog';

import './execution-timer-panel.scss';

export interface ExecutionTimerPanelProps {
  execution: Execution | undefined;
  onCancel(): void;
}

export const ExecutionTimerPanel = React.memo(function ExecutionTimerPanel(
  props: ExecutionTimerPanelProps,
) {
  const { execution, onCancel } = props;
  const [showCancelConfirm, setShowCancelConfirm] = useState(false);
  const [mountTime] = useState(Date.now());
  const [currentTime, setCurrentTime] = useState(Date.now());

  useInterval(() => {
    setCurrentTime(Date.now());
  }, 25);

  function cancelMaybeConfirm() {
    if (execution?.isProcessingData()) {
      setShowCancelConfirm(true);
    } else {
      onCancel();
    }
  }

  const elapsed = currentTime - (execution?.startTime?.valueOf() || mountTime);
  if (elapsed <= 0) return null;
  return (
    <ButtonGroup className="execution-timer-panel">
      <Button
        className="timer"
        icon={IconNames.STOPWATCH}
        text={formatDurationHybrid(elapsed)}
        minimal
      />
      <Button icon={IconNames.CROSS} minimal onClick={cancelMaybeConfirm} />
      {showCancelConfirm && (
        <CancelQueryDialog onCancel={onCancel} onDismiss={() => setShowCancelConfirm(false)} />
      )}
    </ButtonGroup>
  );
});
