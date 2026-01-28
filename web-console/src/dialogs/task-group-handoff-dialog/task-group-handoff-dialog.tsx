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

import { ArrayInput } from '../../components';
import { Api } from '../../singletons';
import { AsyncActionDialog } from '../async-action-dialog/async-action-dialog';

export interface TaskGroupHandoffDialogProps {
  supervisorId: string;
  onSuccess(): void;
  onClose(): void;
}

export function TaskGroupHandoffDialog(props: TaskGroupHandoffDialogProps) {
  const { supervisorId, onSuccess, onClose } = props;
  const [groupIds, setGroupIds] = useState<string[]>([]);

  return (
    <AsyncActionDialog
      action={async () => {
        const resp = await Api.instance.post(
          `/druid/indexer/v1/supervisor/${Api.encodePath(supervisorId)}/taskGroups/handoff`,
          { taskGroupIds: groupIds.map(x => parseInt(x, 10)) },
        );
        return resp.data;
      }}
      confirmButtonText="Handoff task groups"
      successText="Task group handoff has been initiated"
      failText="Could not initiate handoff for the selected task groups"
      intent={Intent.PRIMARY}
      onClose={onClose}
      onSuccess={onSuccess}
    >
      <p>
        Are you sure you want to initiate early handoff for the given task groups belonging to
        supervisor <Tag minimal>{supervisorId}</Tag>?
      </p>
      <FormGroup label="Task group IDs">
        <ArrayInput
          values={groupIds}
          onChange={v => setGroupIds(v || [])}
          placeholder="0, 1, 2, ..."
        />
      </FormGroup>
    </AsyncActionDialog>
  );
}
