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
  onSave: (config: any) => void;
  onDelete: () => void;
  datasource: string;
  configData: any;
}

export interface CompactionDialogState {
  currentConfig?: Record<string, any>;
  allJSONValid: boolean;
}

export class CompactionDialog extends React.PureComponent<
  CompactionDialogProps,
  CompactionDialogState
> {
  constructor(props: CompactionDialogProps) {
    super(props);
    this.state = {
      allJSONValid: true,
    };
  }

  componentDidMount(): void {
    const { datasource, configData } = this.props;
    let config: Record<string, any> = {
      dataSource: datasource,
      inputSegmentSizeBytes: 419430400,
      maxNumSegmentsToCompact: 150,
      skipOffsetFromLatest: 'P1D',
      targetCompactionSizeBytes: 419430400,
      taskContext: null,
      taskPriority: 25,
      tuningConfig: null,
    };
    if (configData !== undefined) {
      config = configData;
    }
    this.setState({
      currentConfig: config,
    });
  }

  render(): JSX.Element {
    const { onClose, onSave, onDelete, datasource, configData } = this.props;
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
              disabled={configData === undefined}
            />
            <Button text="Close" onClick={onClose} />
            <Button
              text="Submit"
              intent={Intent.PRIMARY}
              onClick={() => onSave(currentConfig)}
              disabled={!currentConfig || !allJSONValid}
            />
          </div>
        </div>
      </Dialog>
    );
  }
}
