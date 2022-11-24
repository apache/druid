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

import { Code, FormGroup, Intent } from '@blueprintjs/core';
import React, { useState } from 'react';

import { SuggestibleInput } from '../../components/suggestible-input/suggestible-input';
import { Api } from '../../singletons';
import { uniq } from '../../utils';
import { AsyncActionDialog } from '../async-action-dialog/async-action-dialog';

function getStartOfDay(): string {
  return new Date().toISOString().slice(0, 10);
}

function getStartOfMonth(): string {
  return new Date().toISOString().slice(0, 7) + '-01';
}

function getStartOfYear(): string {
  return new Date().toISOString().slice(0, 4) + '-01-01';
}

export interface KillDatasourceDialogProps {
  datasource: string;
  onClose(): void;
  onSuccess(): void;
}

export const KillDatasourceDialog = function KillDatasourceDialog(
  props: KillDatasourceDialogProps,
) {
  const { datasource, onClose, onSuccess } = props;
  const [interval, setInterval] = useState<string>(`1000-01-01/${getStartOfDay()}`);

  return (
    <AsyncActionDialog
      className="kill-datasource-dialog"
      action={async () => {
        const resp = await Api.instance.delete(
          `/druid/coordinator/v1/datasources/${Api.encodePath(
            datasource,
          )}?kill=true&interval=${Api.encodePath(interval)}`,
          {},
        );
        return resp.data;
      }}
      confirmButtonText="Permanently delete unused segments"
      successText="Kill task was issued. Unused segments in datasource will be deleted"
      failText="Failed submit kill task"
      intent={Intent.DANGER}
      onClose={onClose}
      onSuccess={onSuccess}
      warningChecks={[
        <>
          I understand that this operation will delete all metadata about the unused segments of{' '}
          <Code>{datasource}</Code> and removes them from deep storage.
        </>,
        'I understand that this operation cannot be undone.',
      ]}
    >
      <p>
        Are you sure you want to permanently delete unused segments in <Code>{datasource}</Code>?
      </p>
      <p>This action is not reversible and the data deleted will be lost.</p>
      <FormGroup label="Interval to delete">
        <SuggestibleInput
          value={interval}
          onValueChange={s => setInterval(s || '')}
          suggestions={uniq([
            '1000-01-01/3000-01-01',
            `1000-01-01/${getStartOfDay()}`,
            `1000-01-01/${getStartOfMonth()}`,
            `1000-01-01/${getStartOfYear()}`,
          ])}
        />
      </FormGroup>
    </AsyncActionDialog>
  );
};
