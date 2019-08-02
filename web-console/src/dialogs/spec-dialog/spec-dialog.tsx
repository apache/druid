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
import AceEditor from 'react-ace';

import './spec-dialog.scss';

export interface SpecDialogProps {
  onSubmit: (spec: JSON) => void;
  onClose: () => void;
  title: string;
  initSpec?: any;
}

export interface SpecDialogState {
  spec: string;
}

export class SpecDialog extends React.PureComponent<SpecDialogProps, SpecDialogState> {
  static validJson(json: string): boolean {
    try {
      JSON.parse(json);
      return true;
    } catch (e) {
      return false;
    }
  }

  constructor(props: SpecDialogProps) {
    super(props);
    this.state = {
      spec: props.initSpec ? JSON.stringify(props.initSpec, null, 2) : '{\n\n}',
    };
  }

  private postSpec(): void {
    const { onClose, onSubmit } = this.props;
    const { spec } = this.state;
    if (!SpecDialog.validJson(spec)) return;
    onSubmit(JSON.parse(spec));
    onClose();
  }

  render(): JSX.Element {
    const { onClose, title } = this.props;
    const { spec } = this.state;

    return (
      <Dialog
        className="spec-dialog"
        isOpen
        onClose={onClose}
        title={title}
        canOutsideClickClose={false}
      >
        <AceEditor
          mode="json"
          theme="solarized_dark"
          className="spec-dialog-textarea"
          onChange={e => {
            this.setState({ spec: e });
          }}
          fontSize={12}
          showPrintMargin={false}
          showGutter
          highlightActiveLine
          value={spec}
          width="100%"
          setOptions={{
            showLineNumbers: true,
            tabSize: 2,
          }}
          style={{}}
        />
        <div className={Classes.DIALOG_FOOTER}>
          <div className={Classes.DIALOG_FOOTER_ACTIONS}>
            <Button text="Close" onClick={onClose} />
            <Button
              text="Submit"
              intent={Intent.PRIMARY}
              onClick={() => this.postSpec()}
              disabled={!SpecDialog.validJson(spec)}
            />
          </div>
        </div>
      </Dialog>
    );
  }
}
