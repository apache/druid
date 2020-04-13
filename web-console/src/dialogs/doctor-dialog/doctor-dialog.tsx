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

import { Button, Callout, Classes, Dialog, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';

import { delay, pluralIfNeeded } from '../../utils';

import { DOCTOR_CHECKS } from './doctor-checks';

import './doctor-dialog.scss';

interface Diagnosis {
  type: 'suggestion' | 'issue';
  check: string;
  message: string;
}

export interface DoctorDialogProps {
  onClose: () => void;
}

export interface DoctorDialogState {
  currentCheckIndex?: number;
  diagnoses?: Diagnosis[];
  earlyTermination?: string;
}

export class DoctorDialog extends React.PureComponent<DoctorDialogProps, DoctorDialogState> {
  private mounted = false;

  constructor(props: DoctorDialogProps, context: any) {
    super(props, context);
    this.state = {};
  }

  componentDidMount(): void {
    this.mounted = true;
  }

  componentWillUnmount(): void {
    this.mounted = false;
  }

  async doChecks() {
    this.setState({ currentCheckIndex: 0, diagnoses: [] });

    const addToDiagnoses = (diagnosis: Diagnosis) => {
      if (!this.mounted) return;
      this.setState(oldState => ({
        diagnoses: (oldState.diagnoses || []).concat(diagnosis),
      }));
    };

    for (let i = 0; i < DOCTOR_CHECKS.length; i++) {
      if (!this.mounted) return;
      this.setState({ currentCheckIndex: i });
      const check = DOCTOR_CHECKS[i];
      let terminateChecks = false;

      try {
        await Promise.all([
          await delay(450), // Make sure that a test takes at least this long so that the user can read the test name in the GUI,
          check.check({
            addSuggestion: (message: string) => {
              addToDiagnoses({
                type: 'suggestion',
                check: check.name,
                message,
              });
            },
            addIssue: (message: string) => {
              addToDiagnoses({
                type: 'issue',
                check: check.name,
                message,
              });
            },
            terminateChecks: () => {
              if (!this.mounted) return;
              this.setState({
                earlyTermination: `The check "${check.name}" early terminated the check suite as it has encountered a condition that would make the rest of the tests meaningless.`,
              });
              terminateChecks = true;
            },
          }),
        ]);
      } catch (e) {
        addToDiagnoses({
          type: 'issue',
          check: check.name,
          message: `The check "${check.name}" encountered an unhandled exception. Please report this issue. Message: ${e.message}`,
        });
      }

      if (terminateChecks) break;
    }

    if (!this.mounted) return;
    this.setState({ currentCheckIndex: undefined });
  }

  renderContent() {
    const { diagnoses, currentCheckIndex, earlyTermination } = this.state;

    if (diagnoses) {
      let note: string;
      if (typeof currentCheckIndex === 'number') {
        note = `Running check ${currentCheckIndex + 1}/${DOCTOR_CHECKS.length}: ${
          DOCTOR_CHECKS[currentCheckIndex].name
        }`;
      } else if (earlyTermination) {
        note = `Checks stopped abruptly`;
      } else {
        note = `All ${pluralIfNeeded(DOCTOR_CHECKS.length, 'check')} completed`;
      }

      return (
        <>
          <Callout className="diagnosis">{note}</Callout>
          {diagnoses.map((diagnosis, i) => {
            return (
              <Callout
                key={i}
                className="diagnosis"
                icon={diagnosis.type === 'suggestion' ? IconNames.FLAG : IconNames.WARNING_SIGN}
                intent={diagnosis.type === 'suggestion' ? Intent.NONE : Intent.WARNING}
              >
                {diagnosis.message}
              </Callout>
            );
          })}
          {earlyTermination && (
            <Callout className="diagnosis" intent={Intent.DANGER}>
              {earlyTermination}
            </Callout>
          )}
          {!earlyTermination && currentCheckIndex == null && diagnoses.length === 0 && (
            <Callout className="diagnosis" intent={Intent.SUCCESS}>
              No issues detected
            </Callout>
          )}
        </>
      );
    } else {
      return (
        <div className="analyze-bar">
          <Callout className="diagnosis">
            Automated checks to troubleshoot issues with the cluster.
          </Callout>
          <Button
            text="Analyze Druid cluster"
            intent={Intent.PRIMARY}
            fill
            onClick={() => this.doChecks()}
          />
        </div>
      );
    }
  }

  render(): JSX.Element {
    const { onClose } = this.props;

    return (
      <Dialog
        className="doctor-dialog"
        icon={IconNames.PULSE}
        onClose={onClose}
        title="Druid Doctor"
        isOpen
        canEscapeKeyClose={false}
        canOutsideClickClose={false}
      >
        <div className={Classes.DIALOG_BODY}>{this.renderContent()}</div>
        <div className={Classes.DIALOG_FOOTER}>
          <div className={Classes.DIALOG_FOOTER_ACTIONS}>
            <Button onClick={onClose}>Close</Button>
          </div>
        </div>
      </Dialog>
    );
  }
}
