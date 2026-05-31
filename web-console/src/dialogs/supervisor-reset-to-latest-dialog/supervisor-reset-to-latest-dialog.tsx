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

import { FormGroup, Intent, Tag } from '@blueprintjs/core';
import { useState } from 'react';

import { FancyNumericInput } from '../../components/fancy-numeric-input/fancy-numeric-input';
import { Api } from '../../singletons';
import { AsyncActionDialog } from '../async-action-dialog/async-action-dialog';

export interface SupervisorResetToLatestDialogProps {
  supervisorId: string;
  onClose(): void;
  onSuccess(): void;
}

export function SupervisorResetToLatestDialog(props: SupervisorResetToLatestDialogProps) {
  const { supervisorId, onClose, onSuccess } = props;
  const [backfillTaskCount, setBackfillTaskCount] = useState<number | undefined>();

  return (
    <AsyncActionDialog
      action={async () => {
        const url = `/druid/indexer/v1/supervisor/${Api.encodePath(
          supervisorId,
        )}/resetToLatestAndBackfill${
          backfillTaskCount !== undefined ? `?backfillTaskCount=${backfillTaskCount}` : ''
        }`;
        const resp = await Api.instance.post(url, {});
        return resp.data;
      }}
      confirmButtonText="Reset to latest and backfill"
      successText="Supervisor has been reset to latest and backfill initiated"
      failText="Could not reset supervisor to latest and backfill"
      intent={Intent.DANGER}
      onClose={onClose}
      onSuccess={onSuccess}
      warningChecks={[
        <>
          I understand that resetting <Tag minimal>{supervisorId}</Tag> may result in duplicate data
          due to in-flight tasks at the time of reset.
        </>,
        'I understand that this operation cannot be undone.',
      ]}
    >
      <p>
        Are you sure you want to reset supervisor <Tag minimal>{supervisorId}</Tag> to the latest
        offsets and initiate a backfill?
      </p>
      <p>
        The supervisor will resume from the latest offsets while a backfill supervisor processes the
        skipped range. Duplicate data may occur due to in-flight tasks at the time of reset.
      </p>
      <FormGroup
        label="Backfill task count"
        helperText="Number of tasks to use for the backfill. Defaults to the task count in the supervisor spec."
      >
        <FancyNumericInput
          value={backfillTaskCount}
          onValueChange={v => setBackfillTaskCount(v)}
          onValueEmpty={() => setBackfillTaskCount(undefined)}
          min={1}
          placeholder="Use supervisor spec default"
          fill
        />
      </FormGroup>
    </AsyncActionDialog>
  );
}
