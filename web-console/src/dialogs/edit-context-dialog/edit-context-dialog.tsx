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
import { Button, ButtonGroup, Callout, Classes, Dialog, Intent, TextArea } from '@blueprintjs/core';
import React from 'react';

import { validJson } from '../../utils';
import { QueryContext } from '../../utils/query-context';

import './edit-context-dialog.scss';

export interface EditContextDialogProps {
  queryContext: QueryContext;
  onSubmit: (queryContext: {}) => void;
  onClose: () => void;
}

export interface EditContextDialogState {
  queryContextString: string;
  error?: string;
}

export class EditContextDialog extends React.PureComponent<
  EditContextDialogProps,
  EditContextDialogState
> {
  constructor(props: EditContextDialogProps) {
    super(props);
    this.state = {
      queryContextString: Object.keys(props.queryContext).length
        ? JSON.stringify(props.queryContext, undefined, 2)
        : '{\n\n}',
      error: '',
    };
  }

  private onTextChange = (e: any) => {
    let { error } = this.state;
    const queryContextText = (e.target as HTMLInputElement).value;
    error = undefined;
    let queryContextObject;
    try {
      queryContextObject = JSON.parse(queryContextText);
    } catch (e) {
      error = e.message;
    }

    if (!(typeof queryContextObject === 'object')) {
      error = 'Input is not a valid object';
    }

    this.setState({
      queryContextString: queryContextText,
      error,
    });
  };

  render(): JSX.Element {
    const { onClose, onSubmit } = this.props;
    const { queryContextString } = this.state;
    let { error } = this.state;

    let queryContextObject: {} | undefined;
    try {
      queryContextObject = JSON.parse(queryContextString);
    } catch (e) {
      error = e.message;
    }

    if (!(typeof queryContextObject === 'object') && !error) {
      error = 'Input is not a valid object';
    }

    return (
      <Dialog
        className="edit-context-dialog"
        isOpen
        onClose={() => onClose()}
        title={'Edit query context'}
      >
        <TextArea value={queryContextString} onChange={this.onTextChange} autoFocus />
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          {error && (
            <Callout intent={Intent.DANGER} className="edit-context-dialog-error">
              {error}
            </Callout>
          )}
          <ButtonGroup className={'edit-context-dialog-buttons'}>
            <Button
              text={'Close'}
              onClick={() => {
                onClose();
              }}
            />
            <Button
              disabled={!validJson(queryContextString) || typeof queryContextObject !== 'object'}
              text={'Submit'}
              intent={Intent.PRIMARY}
              onClick={() => (queryContextObject ? onSubmit(queryContextObject) : null)}
            />
          </ButtonGroup>
        </div>
      </Dialog>
    );
  }
}
