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
import { Button, Classes, Dialog, Intent, TextArea } from '@blueprintjs/core';
import React from 'react';

import { QueryContext } from '../../utils/query-context';

import './edit-context-dialog.scss';

export interface EditContextDialogProps {
  queryContext: QueryContext;
  onSubmit: (queryContext: QueryContext) => void;
  onClose: () => void;
}

export interface EditContextDialogState {
  queryContextText: string;
  error?: boolean;
  queryContextUpdated: QueryContext;
}

export class EditContextDialog extends React.PureComponent<
  EditContextDialogProps,
  EditContextDialogState
> {
  constructor(props: EditContextDialogProps) {
    super(props);
    this.state = {
      queryContextText: Object.keys(props.queryContext).length
        ? JSON.stringify(props.queryContext, undefined, 2)
        : '{\n\n}',
      queryContextUpdated: props.queryContext ? props.queryContext : {},
    };
  }

  private onTextChange = (e: any) => {
    let { error } = this.state;
    const queryContextText = (e.target as HTMLInputElement).value;

    try {
      this.setState({ queryContextUpdated: JSON.parse(queryContextText) });
    } catch {
      error = true;
    }

    this.setState({
      queryContextText,
      error,
    });
  };

  render(): JSX.Element {
    const { onClose, onSubmit } = this.props;
    const { queryContextText, error, queryContextUpdated } = this.state;

    return (
      <Dialog
        className="edit-context-dialog"
        isOpen
        onClose={() => onClose()}
        title={'Edit Query Context'}
      >
        <TextArea value={queryContextText} onChange={this.onTextChange} autoFocus />
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button
            disabled={error}
            text={'Close'}
            intent={Intent.PRIMARY}
            onClick={() => {
              onClose();
            }}
          />
          <Button
            disabled={error}
            text={'Submit'}
            intent={Intent.PRIMARY}
            onClick={() => {
              onSubmit(queryContextUpdated);
            }}
          />
        </div>
      </Dialog>
    );
  }
}
