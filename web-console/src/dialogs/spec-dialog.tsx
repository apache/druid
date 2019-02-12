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

import * as React from "react";
import axios from 'axios';
import {Button, Classes, Dialog, Intent, EditableText} from "@blueprintjs/core";
import "./spec-dialog.scss"
import {QueryManager} from "../utils";

export interface SpecDialogProps extends React.Props<any> {
  isOpen: boolean;
  onSubmit: (spec: JSON) => void;
  onClose: () => void;
  title: string;
}

export interface SpecDialogState {
  spec: string;
}

export class SpecDialog extends React.Component<SpecDialogProps, SpecDialogState> {
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
      spec: ""
    }
  }

  private postSpec(): void {
    const { onClose, onSubmit } = this.props;
    const { spec } = this.state;
    if (!SpecDialog.validJson(spec)) return;
    onSubmit(JSON.parse(spec));
    onClose();
  }

  render() {
    const { isOpen, onClose, title } = this.props;
    const { spec } = this.state;

    return <Dialog
      className={"post-spec-dialog"}
      isOpen={isOpen}
      onClose={onClose}
      title={title}
    >
      <EditableText
        className={"post-spec-dialog-textarea"}
        multiline={true}
        minLines={30}
        maxLines={30}
        placeholder={"Enter the spec JSON to post"}
        onChange={ (e) => {this.setState({ spec: e })}}
      />
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button
            text="Close"
            onClick={onClose}
          />
          <Button
            text="Submit"
            intent={Intent.PRIMARY}
            onClick={() => this.postSpec()}
            disabled={!SpecDialog.validJson(spec)}
          />
        </div>
      </div>
    </Dialog>;
  }
}
