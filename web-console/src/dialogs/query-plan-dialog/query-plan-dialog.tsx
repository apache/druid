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
  InputGroup,
  Intent,
  TextArea,
} from '@blueprintjs/core';
import React from 'react';

import { BasicQueryExplanation, SemiJoinQueryExplanation } from '../../utils';

import './query-plan-dialog.scss';

export interface QueryPlanDialogProps {
  explainResult?: BasicQueryExplanation | SemiJoinQueryExplanation | string;
  explainError?: string;
  onClose: () => void;
  setQueryString: (queryString: string) => void;
}

export interface QueryPlanDialogState {}

export class QueryPlanDialog extends React.PureComponent<
  QueryPlanDialogProps,
  QueryPlanDialogState
> {
  constructor(props: QueryPlanDialogProps) {
    super(props);
    this.state = {};
  }

  private queryString: string = '';

  render(): JSX.Element {
    const { explainResult, explainError, onClose, setQueryString } = this.props;

    let content: JSX.Element;

    if (explainError) {
      content = <div>{explainError}</div>;
    } else if (!explainResult) {
      content = <div />;
    } else if ((explainResult as BasicQueryExplanation).query) {
      let signature: JSX.Element | undefined;
      if ((explainResult as BasicQueryExplanation).signature) {
        const signatureContent = (explainResult as BasicQueryExplanation).signature || '';
        signature = (
          <FormGroup label="Signature">
            <InputGroup defaultValue={signatureContent} readOnly />
          </FormGroup>
        );
      }

      this.queryString = JSON.stringify(
        (explainResult as BasicQueryExplanation).query[0],
        undefined,
        2,
      );
      content = (
        <div className="one-query">
          <FormGroup label="Query">
            <TextArea readOnly value={this.queryString} />
          </FormGroup>
          {signature}
        </div>
      );
    } else if (
      (explainResult as SemiJoinQueryExplanation).mainQuery &&
      (explainResult as SemiJoinQueryExplanation).subQueryRight
    ) {
      let mainSignature: JSX.Element | undefined;
      let subSignature: JSX.Element | undefined;
      if ((explainResult as SemiJoinQueryExplanation).mainQuery.signature) {
        const signatureContent =
          (explainResult as SemiJoinQueryExplanation).mainQuery.signature || '';
        mainSignature = (
          <FormGroup label="Signature">
            <InputGroup defaultValue={signatureContent} readOnly />
          </FormGroup>
        );
      }
      if ((explainResult as SemiJoinQueryExplanation).subQueryRight.signature) {
        const signatureContent =
          (explainResult as SemiJoinQueryExplanation).subQueryRight.signature || '';
        subSignature = (
          <FormGroup label="Signature">
            <InputGroup defaultValue={signatureContent} readOnly />
          </FormGroup>
        );
      }

      content = (
        <div className="two-queries">
          <FormGroup label="Main query">
            <TextArea
              readOnly
              value={JSON.stringify(
                (explainResult as SemiJoinQueryExplanation).mainQuery.query,
                undefined,
                2,
              )}
            />
          </FormGroup>
          {mainSignature}
          <FormGroup label="Sub query">
            <TextArea
              readOnly
              value={JSON.stringify(
                (explainResult as SemiJoinQueryExplanation).subQueryRight.query,
                undefined,
                2,
              )}
            />
          </FormGroup>
          {subSignature}
        </div>
      );
    } else {
      content = <div>{explainResult}</div>;
    }

    return (
      <Dialog className="query-plan-dialog" isOpen onClose={onClose} title="Query plan">
        <div className={Classes.DIALOG_BODY}>{content}</div>
        <div className={Classes.DIALOG_FOOTER}>
          <div className={Classes.DIALOG_FOOTER_ACTIONS}>
            <Button text="Close" onClick={onClose} />
            <Button
              text="Open"
              intent={Intent.PRIMARY}
              onClick={() => setQueryString(this.queryString)}
            />
          </div>
        </div>
      </Dialog>
    );
  }
}
