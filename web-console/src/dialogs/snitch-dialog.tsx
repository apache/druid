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

import * as React from 'react';
import {
  FormGroup,
  Button,
  InputGroup,
  Dialog,
  IDialogProps,
  Classes,
  Intent,
} from "@blueprintjs/core";

import { IconNames } from "@blueprintjs/icons";

export interface SnitchDialogProps extends IDialogProps {
  onSave: (author: string, comment: string) => void;
  saveDisabled?: boolean;
  onReset?: () => void;
}

export interface SnitchDialogState {
  author: string;
  comment: string;

  showFinalStep?: boolean;
  saveDisabled?: boolean;
}

export class SnitchDialog extends React.Component<SnitchDialogProps, SnitchDialogState> {
  constructor(props: SnitchDialogProps) {
    super(props);

    this.state = {
      comment: "",
      author: "",
      saveDisabled: true
    }
  }

  save = () => {
    const { onSave, onClose } = this.props;
    const { author, comment } = this.state;

    onSave(author, comment);
    if (onClose) onClose();
  }

  changeAuthor(newAuthor: string)  {
    const { author, comment } = this.state;

    this.setState({
      author: newAuthor,
      saveDisabled: !newAuthor || !comment
    });
  }

  changeComment(newComment: string)  {
    const { author, comment } = this.state;

    this.setState({
      comment: newComment,
      saveDisabled: !author || !newComment
    });
  }

  reset = () => {
    const { onReset } = this.props;

    if (onReset) onReset();
  }

  back = () => {
    this.setState({
      showFinalStep: false
    });
  }

  goToFinalStep = () => {
    this.setState({
      showFinalStep: true
    });
  }

  renderFinalStep() {
    const { onClose, children } = this.props;
    const { saveDisabled, author, comment } = this.state;

    return <Dialog {...this.props}>
      <div className={`dialog-body ${Classes.DIALOG_BODY}`}>
        <FormGroup label={"Who is making this change?"}>
          <InputGroup value={author} onChange={(e: any) => this.changeAuthor(e.target.value)}/>
        </FormGroup>
        <FormGroup className={"comment"}>
          <InputGroup
            value={comment}
            placeholder={"Why are you making this change?"}
            onChange={(e: any) => this.changeComment(e.target.value)}
            large={true}
          />
        </FormGroup>
      </div>

      <div className={Classes.DIALOG_FOOTER}>
        {this.renderActions(saveDisabled)}
      </div>
    </Dialog>;
  }

  renderActions(saveDisabled?: boolean) {
    const { onReset } = this.props;
    const { showFinalStep } = this.state;

    return <div className={Classes.DIALOG_FOOTER_ACTIONS}>
      <FormGroup>
        { showFinalStep
          ? <Button onClick={this.back} icon={IconNames.ARROW_LEFT}>Back</Button>
          : onReset ? <Button onClick={this.reset} intent={"none"}>Reset</Button> : null
        }

        { showFinalStep
          ? <Button disabled={saveDisabled} text="Save" onClick={this.save} intent={Intent.PRIMARY} rightIcon={IconNames.TICK}/>
          : <Button disabled={saveDisabled} text="Next" onClick={this.goToFinalStep} intent={Intent.PRIMARY} rightIcon={IconNames.ARROW_RIGHT}/>
        }
      </FormGroup>
    </div>
  }

  onOpening = (node: HTMLElement) => {
    const { onOpening } = this.props;

    this.setState({
      author: '',
      comment: ''
    });

    onOpening && onOpening(node);
  }

  render() {
    const { isOpen, onClose, className, children, saveDisabled } = this.props;
    const { showFinalStep } = this.state;

    if (showFinalStep) return this.renderFinalStep();

    return <Dialog {...this.props} onOpening={this.onOpening}>
      <div className={Classes.DIALOG_BODY}>
        {children}
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        {this.renderActions(saveDisabled)}
      </div>
    </Dialog>;
  }
}
