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
import React from 'react';

import { AutoForm, ExternalLink } from '../../components';

import './compaction-dialog.scss';

export interface CompactionDialogProps {
  onClose: () => void;
  onSave: (config: Record<string, any>) => void;
  onDelete: () => void;
  datasource: string;
  compactionConfig?: Record<string, any>;
}

export interface CompactionDialogState {
  currentConfig?: Record<string, any>;
  allJsonValid: boolean;
}

export class CompactionDialog extends React.PureComponent<
  CompactionDialogProps,
  CompactionDialogState
> {
  static DEFAULT_MAX_ROWS_PER_SEGMENT = 5000000;

  constructor(props: CompactionDialogProps) {
    super(props);
    this.state = {
      allJsonValid: true,
    };
  }

  componentDidMount(): void {
    const { datasource, compactionConfig } = this.props;

    this.setState({
      currentConfig: compactionConfig || {
        dataSource: datasource,
      },
    });
  }

  private handleSubmit = () => {
    const { onSave } = this.props;
    const { currentConfig } = this.state;
    if (!currentConfig) return;

    onSave(currentConfig);
  };

  render(): JSX.Element {
    const { onClose, onDelete, datasource, compactionConfig } = this.props;
    const { currentConfig, allJsonValid } = this.state;

    return (
      <Dialog
        className="compaction-dialog"
        isOpen
        onClose={onClose}
        canOutsideClickClose={false}
        title={`Compaction config: ${datasource}`}
      >
        <AutoForm
          fields={[
            {
              name: 'inputSegmentSizeBytes',
              type: 'number',
              defaultValue: 419430400,
              info: (
                <p>
                  Maximum number of total segment bytes processed per compaction task. Since a time
                  chunk must be processed in its entirety, if the segments for a particular time
                  chunk have a total size in bytes greater than this parameter, compaction will not
                  run for that time chunk. Because each compaction task runs with a single thread,
                  setting this value too far above 1–2GB will result in compaction tasks taking an
                  excessive amount of time.
                </p>
              ),
            },
            {
              name: 'skipOffsetFromLatest',
              type: 'string',
              defaultValue: 'P1D',
              info: (
                <p>
                  The offset for searching segments to be compacted. Strongly recommended to set for
                  realtime dataSources.
                </p>
              ),
            },
            {
              name: 'maxRowsPerSegment',
              type: 'number',
              defaultValue: CompactionDialog.DEFAULT_MAX_ROWS_PER_SEGMENT,
              info: (
                <p>
                  The target segment size, for each segment, after compaction. The actual sizes of
                  compacted segments might be slightly larger or smaller than this value. Each
                  compaction task may generate more than one output segment, and it will try to keep
                  each output segment close to this configured size.
                </p>
              ),
            },
            {
              name: 'taskContext',
              type: 'json',
              info: (
                <p>
                  <ExternalLink href="https://druid.apache.org/docs/latest/ingestion/tasks.html#task-context">
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
                  <ExternalLink href="https://druid.apache.org/docs/latest/configuration/index.html#compact-task-tuningconfig">
                    Tuning config
                  </ExternalLink>{' '}
                  for compaction tasks.
                </p>
              ),
            },
          ]}
          model={currentConfig}
          onChange={m => this.setState({ currentConfig: m })}
          updateJsonValidity={e => this.setState({ allJsonValid: e })}
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
              onClick={this.handleSubmit}
              disabled={!currentConfig || !allJsonValid}
            />
          </div>
        </div>
      </Dialog>
    );
  }
}
