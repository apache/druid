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

import { AutoForm } from '../../components';

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
  allJSONValid: boolean;
}

export class CompactionDialog extends React.PureComponent<
  CompactionDialogProps,
  CompactionDialogState
> {
  static DEFAULT_TARGET_COMPACTION_SIZE_BYTES = 419430400;

  constructor(props: CompactionDialogProps) {
    super(props);
    this.state = {
      allJSONValid: true,
    };
  }

  componentDidMount(): void {
    const { datasource, compactionConfig } = this.props;
    let config: Record<string, any> = {
      dataSource: datasource,
      inputSegmentSizeBytes: 419430400,
      maxNumSegmentsToCompact: 150,
      skipOffsetFromLatest: 'P1D',
      targetCompactionSizeBytes: CompactionDialog.DEFAULT_TARGET_COMPACTION_SIZE_BYTES,
      taskContext: null,
      taskPriority: 25,
      tuningConfig: null,
    };
    if (compactionConfig !== undefined) {
      config = compactionConfig;
    }
    this.setState({
      currentConfig: config,
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
    const { currentConfig, allJSONValid } = this.state;
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
            },
            {
              name: 'maxNumSegmentsToCompact',
              type: 'number',
            },
            {
              name: 'skipOffsetFromLatest',
              type: 'string',
            },
            {
              name: 'targetCompactionSizeBytes',
              type: 'number',
            },
            {
              name: 'taskContext',
              type: 'json',
            },
            {
              name: 'taskPriority',
              type: 'number',
            },
            {
              name: 'tuningConfig',
              type: 'json',
            },
          ]}
          model={currentConfig}
          onChange={m => this.setState({ currentConfig: m })}
          updateJSONValidity={e => this.setState({ allJSONValid: e })}
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
              disabled={!currentConfig || !allJSONValid}
            />
          </div>
        </div>
      </Dialog>
    );
  }
}
