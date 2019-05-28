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

import {
  Button,
  Classes,
  Dialog,
  FormGroup,
  IDialogProps,
  InputGroup,
  Intent
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import classNames = require('classnames');
import * as React from 'react';

import { HistoryDialog } from '../history-dialog/history-dialog';

import './snitch-dialog.scss';

export interface SnitchDialogProps extends IDialogProps {
  onSave: (comment: string) => void;
  saveDisabled?: boolean;
  onReset?: () => void;
  historyRecords?: any[];
}

export interface SnitchDialogState {
  comment: string;

  showFinalStep?: boolean;
  saveDisabled?: boolean;

  showHistory?: boolean;
}

export class SnitchDialog extends React.Component<SnitchDialogProps, SnitchDialogState> {
  constructor(props: SnitchDialogProps) {
    super(props);

    this.state = {
      comment: '',
      saveDisabled: true
    };
  }

  save = () => {
    const { onSave, onClose } = this.props;
    const { comment } = this.state;

    onSave(comment);
    if (onClose) onClose();
  }

  changeComment(newComment: string)  {
    const { comment } = this.state;

    this.setState({
      comment: newComment,
      saveDisabled: !newComment
    });
  }

  reset = () => {
    const { onReset } = this.props;

    if (onReset) onReset();
  }

  back = () => {
    this.setState({
      showFinalStep: false,
      showHistory: false
    });
  }

  goToFinalStep = () => {
    this.setState({
      showFinalStep: true
    });
  }

  goToHistory = () => {
    this.setState({
      showHistory: true
    });
  }

  renderFinalStep() {
    const { onClose, children } = this.props;
    const { saveDisabled, comment } = this.state;

    return <Dialog {...this.props}>
      <div className={`dialog-body ${Classes.DIALOG_BODY}`}>
        <FormGroup label="Why are you making this change?" className="comment">
          <InputGroup
            large
            value={comment}
            placeholder="Enter description here"
            onChange={(e: any) => this.changeComment(e.target.value)}
          />
        </FormGroup>
      </div>

      <div className={Classes.DIALOG_FOOTER}>
        {this.renderActions(saveDisabled)}
      </div>
    </Dialog>;
  }

  renderHistoryDialog() {
    const { historyRecords } = this.props;
    return <HistoryDialog
      {...this.props}
      className="history-dialog"
      historyRecords={historyRecords}
    >
      <div className={Classes.DIALOG_FOOTER_ACTIONS}>
        <Button onClick={this.back} icon={IconNames.ARROW_LEFT}>Back</Button>
      </div>
    </HistoryDialog>;
  }

  renderActions(saveDisabled?: boolean) {
    const { onReset, historyRecords } = this.props;
    const { showFinalStep } = this.state;

    return <div className={Classes.DIALOG_FOOTER_ACTIONS}>
      {showFinalStep || historyRecords === undefined
        ? null
        : <Button className="left-align-button" minimal text="History" onClick={this.goToHistory}/>
      }

      { showFinalStep
        ? <Button onClick={this.back} icon={IconNames.ARROW_LEFT}>Back</Button>
        : onReset ? <Button onClick={this.reset} intent={'none' as any}>Reset</Button> : null
      }

      { showFinalStep
        ? <Button disabled={saveDisabled} text="Save" onClick={this.save} intent={Intent.PRIMARY as any} rightIcon={IconNames.TICK}/>
        : <Button disabled={saveDisabled} text="Next" onClick={this.goToFinalStep} intent={Intent.PRIMARY as any} rightIcon={IconNames.ARROW_RIGHT}/>
      }
    </div>;
  }

  render() {
    const { onClose, className, children, saveDisabled } = this.props;
    const { showFinalStep, showHistory } = this.state;

    if (showFinalStep) return this.renderFinalStep();
    if (showHistory) return this.renderHistoryDialog();

    const propsClone: any = Object.assign({}, this.props);
    propsClone.className = classNames('snitch-dialog', propsClone.className);
    return <Dialog isOpen {...propsClone}>
      <div className={Classes.DIALOG_BODY}>
        {children}
      </div>

      <div className={Classes.DIALOG_FOOTER}>
        {this.renderActions(saveDisabled)}
      </div>
    </Dialog>;
  }
}
