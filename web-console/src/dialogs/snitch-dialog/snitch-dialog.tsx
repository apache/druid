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

import { Button, Classes, Dialog, FormGroup, InputGroup, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import classNames from 'classnames';
import React from 'react';

import { HistoryDialog } from '../history-dialog/history-dialog';

import './snitch-dialog.scss';

export interface SnitchDialogProps {
  title: string;
  className?: string;
  onSave: (comment: string) => void;
  saveDisabled?: boolean;
  onReset?: () => void;
  onClose: () => void;
  historyRecords?: any[];
}

export interface SnitchDialogState {
  comment: string;

  showFinalStep?: boolean;

  showHistory?: boolean;
}

export class SnitchDialog extends React.PureComponent<SnitchDialogProps, SnitchDialogState> {
  constructor(props: SnitchDialogProps) {
    super(props);

    this.state = {
      comment: '',
    };
  }

  save = () => {
    const { onSave, onClose } = this.props;
    const { comment } = this.state;

    onSave(comment);
    if (onClose) onClose();
  };

  changeComment(newComment: string) {
    this.setState({
      comment: newComment,
    });
  }

  reset = () => {
    const { onReset } = this.props;

    if (onReset) onReset();
  };

  back = () => {
    this.setState({
      showFinalStep: false,
      showHistory: false,
    });
  };

  goToFinalStep = () => {
    this.setState({
      showFinalStep: true,
    });
  };

  handleGoToHistory = () => {
    this.setState({
      showHistory: true,
    });
  };

  renderFinalStep() {
    const { comment } = this.state;

    return (
      <Dialog isOpen {...this.props}>
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
        <div className={Classes.DIALOG_FOOTER}>{this.renderActions(!comment)}</div>
      </Dialog>
    );
  }

  renderHistoryDialog(): JSX.Element | null {
    const { historyRecords } = this.props;
    if (!historyRecords) return null;

    return (
      <HistoryDialog
        {...this.props}
        historyRecords={historyRecords}
        buttons={
          <Button onClick={this.back} icon={IconNames.ARROW_LEFT}>
            Back
          </Button>
        }
      />
    );
  }

  renderActions(saveDisabled?: boolean) {
    const { onReset, historyRecords } = this.props;
    const { showFinalStep } = this.state;

    return (
      <div className={Classes.DIALOG_FOOTER_ACTIONS}>
        {!showFinalStep && historyRecords && (
          <Button
            className="left-align-button"
            minimal
            text="History"
            onClick={this.handleGoToHistory}
          />
        )}

        {showFinalStep ? (
          <Button onClick={this.back} icon={IconNames.ARROW_LEFT}>
            Back
          </Button>
        ) : onReset ? (
          <Button onClick={this.reset} intent={Intent.NONE}>
            Reset
          </Button>
        ) : null}

        {showFinalStep ? (
          <Button
            disabled={saveDisabled}
            text="Save"
            onClick={this.save}
            intent={Intent.PRIMARY as any}
            rightIcon={IconNames.TICK}
          />
        ) : (
          <Button
            disabled={saveDisabled}
            text="Next"
            onClick={this.goToFinalStep}
            intent={Intent.PRIMARY as any}
            rightIcon={IconNames.ARROW_RIGHT}
          />
        )}
      </div>
    );
  }

  render(): JSX.Element | null {
    const { children, saveDisabled } = this.props;
    const { showFinalStep, showHistory } = this.state;

    if (showFinalStep) return this.renderFinalStep();
    if (showHistory) return this.renderHistoryDialog();

    const propsClone: any = Object.assign({}, this.props);
    propsClone.className = classNames('snitch-dialog', propsClone.className);
    return (
      <Dialog isOpen {...propsClone} canOutsideClickClose={false}>
        <div className={Classes.DIALOG_BODY}>{children}</div>
        <div className={Classes.DIALOG_FOOTER}>{this.renderActions(saveDisabled)}</div>
      </Dialog>
    );
  }
}
