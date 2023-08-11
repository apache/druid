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

import { Code, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';

import { AlertDialog } from '../../../dialogs';
import type { CapacityInfo } from '../../../druid-models';
import { formatInteger } from '../../../utils';

export interface CapacityAlertProps {
  maxNumTasks: number;
  capacityInfo: CapacityInfo;
  onRun(): void;
  onClose(): void;
}

export function CapacityAlert(props: CapacityAlertProps) {
  const { maxNumTasks, capacityInfo, onRun, onClose } = props;
  const { usedTaskSlots, totalTaskSlots } = capacityInfo;

  function runAndClose() {
    onRun();
    onClose();
  }

  if (totalTaskSlots < maxNumTasks) {
    return (
      <AlertDialog
        cancelButtonText="Cancel"
        confirmButtonText="Run it anyway"
        icon={IconNames.WARNING_SIGN}
        intent={Intent.WARNING}
        isOpen
        onCancel={onClose}
        onConfirm={runAndClose}
      >
        <p>
          The cluster does not have enough total task slot capacity (
          <Code>{formatInteger(totalTaskSlots)}</Code>) to run this query which is set to use up to{' '}
          <Code>{formatInteger(maxNumTasks)}</Code> tasks. Unless more capacity is added this query
          will stall and never run.
        </p>
      </AlertDialog>
    );
  } else {
    return (
      <AlertDialog
        cancelButtonText="Cancel"
        confirmButtonText="Run it anyway"
        intent={Intent.PRIMARY}
        isOpen
        onCancel={onClose}
        onConfirm={runAndClose}
      >
        <p>
          The cluster does not currently have enough available task slots (current usage:{' '}
          <Code>{`${formatInteger(usedTaskSlots)} of ${formatInteger(totalTaskSlots)}`}</Code>) to
          run this query which is set to use up to <Code>{formatInteger(maxNumTasks)}</Code> tasks.
          This query will have to wait for task slots to become available before running.
        </p>
        <p>Are you sure you want to run it?</p>
      </AlertDialog>
    );
  }
}
