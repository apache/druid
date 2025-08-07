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

import { Intent, Tag } from '@blueprintjs/core';
import { useState } from 'react';

import { FormGroupWithInfo, PopoverText } from '../../components';
import { SuggestibleInput } from '../../components/suggestible-input/suggestible-input';
import { Api } from '../../singletons';
import { uniq } from '../../utils';
import { AsyncActionDialog } from '../async-action-dialog/async-action-dialog';

function getSuggestions(): string[] {
  // Default to a data 24h ago so as not to cause a conflict between streaming ingestion and kill tasks
  const end = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  const startOfDay = end.slice(0, 10);
  const startOfMonth = end.slice(0, 7) + '-01';
  const startOfYear = end.slice(0, 4) + '-01-01';

  return uniq([
    `1000-01-01/${startOfDay}`,
    `1000-01-01/${startOfMonth}`,
    `1000-01-01/${startOfYear}`,
    '1000-01-01/3000-01-01',
  ]);
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
  const suggestions = getSuggestions();
  const [interval, setInterval] = useState<string>(suggestions[0]);

  return (
    <AsyncActionDialog
      className="kill-datasource-dialog"
      action={async () => {
        const resp = await Api.instance.delete(
          `/druid/indexer/v1/datasources/${Api.encodePath(datasource)}/intervals/${Api.encodePath(
            interval.replace(/\//g, '_'),
          )}`,
          {},
        );
        return resp.data;
      }}
      confirmButtonText="Permanently delete unused segments"
      successText={
        <>
          Kill task was issued. Unused segments in datasource <Tag minimal>{datasource}</Tag> will
          be deleted
        </>
      }
      failText="Failed submit kill task"
      intent={Intent.DANGER}
      onClose={onClose}
      onSuccess={onSuccess}
      warningChecks={[
        <>
          I understand that this operation will delete all metadata about the unused segments of{' '}
          <Tag minimal>{datasource}</Tag> and removes them from deep storage.
        </>,
        'I understand that this operation cannot be undone.',
      ]}
    >
      <p>
        Are you sure you want to permanently delete unused segments in{' '}
        <Tag minimal>{datasource}</Tag>?
      </p>
      <p>This action is not reversible and the data deleted will be lost.</p>
      <FormGroupWithInfo
        label="Interval to delete"
        info={
          <PopoverText>
            <p>
              The range of time over which to delete unused segments specified in ISO8601 interval
              format.
            </p>
            <p>
              If you have streaming ingestion running make sure that your interval range does not
              overlap with intervals where streaming data is being added - otherwise the kill task
              will not start.
            </p>
          </PopoverText>
        }
      >
        <SuggestibleInput
          value={interval}
          onValueChange={s => setInterval(s || '')}
          suggestions={suggestions}
        />
      </FormGroupWithInfo>
    </AsyncActionDialog>
  );
};
