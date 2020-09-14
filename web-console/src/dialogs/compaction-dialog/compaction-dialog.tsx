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

import { Button, Classes, Dialog, Intent } from '@blueprintjs/core';
import React, { useState } from 'react';

import { AutoForm, ExternalLink, Field } from '../../components';
import { getLink } from '../../links';

import './compaction-dialog.scss';

export const DEFAULT_MAX_ROWS_PER_SEGMENT = 5000000;

const COMPACTION_CONFIG_FIELDS: Field<Record<string, any>>[] = [
  {
    name: 'inputSegmentSizeBytes',
    type: 'number',
    defaultValue: 419430400,
    info: (
      <p>
        Maximum number of total segment bytes processed per compaction task. Since a time chunk must
        be processed in its entirety, if the segments for a particular time chunk have a total size
        in bytes greater than this parameter, compaction will not run for that time chunk. Because
        each compaction task runs with a single thread, setting this value too far above 1–2GB will
        result in compaction tasks taking an excessive amount of time.
      </p>
    ),
  },
  {
    name: 'skipOffsetFromLatest',
    type: 'string',
    defaultValue: 'P1D',
    info: (
      <p>
        The offset for searching segments to be compacted. Strongly recommended to set for realtime
        dataSources.
      </p>
    ),
  },
  {
    name: 'taskContext',
    type: 'json',
    info: (
      <p>
        <ExternalLink href={`${getLink('DOCS')}/ingestion/tasks.html#task-context`}>
          Task context
        </ExternalLink>{' '}
        for compaction tasks.
      </p>
    ),
  },
  {
    name: 'taskPriority',
    type: 'number',
    defaultValue: 25,
    info: <p>Priority of the compaction task.</p>,
  },
  {
    name: 'tuningConfig',
    type: 'json',
    info: (
      <p>
        <ExternalLink
          href={`${getLink('DOCS')}/configuration/index.html#compact-task-tuningconfig`}
        >
          Tuning config
        </ExternalLink>{' '}
        for compaction tasks.
      </p>
    ),
  },
];

export interface CompactionDialogProps {
  onClose: () => void;
  onSave: (config: Record<string, any>) => void;
  onDelete: () => void;
  datasource: string;
  compactionConfig?: Record<string, any>;
}

export const CompactionDialog = React.memo(function CompactionDialog(props: CompactionDialogProps) {
  const { datasource, compactionConfig, onSave, onClose, onDelete } = props;

  const [currentConfig, setCurrentConfig] = useState<Record<string, any>>(
    compactionConfig || {
      dataSource: datasource,
    },
  );

  function handleSubmit() {
    if (!currentConfig) return;
    onSave(currentConfig);
  }

  return (
    <Dialog
      className="compaction-dialog"
      isOpen
      onClose={onClose}
      canOutsideClickClose={false}
      title={`Compaction config: ${datasource}`}
    >
      <AutoForm
        fields={COMPACTION_CONFIG_FIELDS}
        model={currentConfig}
        onChange={m => setCurrentConfig(m)}
      />
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button
            text="Delete"
            intent={Intent.DANGER}
            onClick={onDelete}
            disabled={!compactionConfig}
          />
          <Button text="Close" onClick={onClose} />
          <Button
            text="Submit"
            intent={Intent.PRIMARY}
            onClick={handleSubmit}
            disabled={!currentConfig}
          />
        </div>
      </div>
    </Dialog>
  );
});
