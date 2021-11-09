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
import * as JSONBig from 'json-bigint-native';
import React from 'react';

import { Loader } from '../../../components';
import { useQueryManager } from '../../../hooks';
import {
  BasicQueryExplanation,
  getDruidErrorMessage,
  parseQueryPlan,
  queryDruidSql,
  QueryWithContext,
  SemiJoinQueryExplanation,
  trimSemicolon,
} from '../../../utils';
import { isEmptyContext } from '../../../utils/query-context';

import './explain-dialog.scss';

function isExplainQuery(query: string): boolean {
  return /EXPLAIN\sPLAN\sFOR/i.test(query);
}

function wrapInExplainIfNeeded(query: string): string {
  query = trimSemicolon(query);
  if (isExplainQuery(query)) return query;
  return `EXPLAIN PLAN FOR (${query}\n)`;
}

export interface ExplainDialogProps {
  queryWithContext: QueryWithContext;
  mandatoryQueryContext?: Record<string, any>;
  onClose: () => void;
  setQueryString: (queryString: string) => void;
}

export const ExplainDialog = React.memo(function ExplainDialog(props: ExplainDialogProps) {
  const { queryWithContext, onClose, setQueryString, mandatoryQueryContext } = props;

  const [explainState] = useQueryManager<
    QueryWithContext,
    BasicQueryExplanation | SemiJoinQueryExplanation | string
  >({
    processQuery: async (queryWithContext: QueryWithContext) => {
      const { queryString, queryContext, wrapQueryLimit } = queryWithContext;

      let context: Record<string, any> | undefined;
      if (!isEmptyContext(queryContext) || wrapQueryLimit || mandatoryQueryContext) {
        context = { ...queryContext, ...(mandatoryQueryContext || {}) };
        if (typeof wrapQueryLimit !== 'undefined') {
          context.sqlOuterLimit = wrapQueryLimit + 1;
        }
      }

      let result: any[] | undefined;
      try {
        result = await queryDruidSql({
          query: wrapInExplainIfNeeded(queryString),
          context,
        });
      } catch (e) {
        throw new Error(getDruidErrorMessage(e));
      }

      return parseQueryPlan(result[0]['PLAN']);
    },
    initQuery: queryWithContext,
  });

  let content: JSX.Element;
  let queryString: string | undefined;

  const { loading, error: explainError, data: explainResult } = explainState;
  if (loading) {
    content = <Loader />;
  } else if (explainError) {
    content = <div>{explainError.message}</div>;
  } else if (!explainResult) {
    content = <div />;
  } else if ((explainResult as BasicQueryExplanation).query) {
    queryString = JSONBig.stringify(
      (explainResult as BasicQueryExplanation).query[0],
      undefined,
      2,
    );
    content = (
      <div className="one-query">
        <FormGroup label="Query">
          <TextArea readOnly value={queryString} />
        </FormGroup>
        {(explainResult as BasicQueryExplanation).signature && (
          <FormGroup label="Signature">
            <InputGroup
              defaultValue={(explainResult as BasicQueryExplanation).signature || ''}
              readOnly
            />
          </FormGroup>
        )}
      </div>
    );
  } else if (
    (explainResult as SemiJoinQueryExplanation).mainQuery &&
    (explainResult as SemiJoinQueryExplanation).subQueryRight
  ) {
    content = (
      <div className="two-queries">
        <FormGroup label="Main query">
          <TextArea
            readOnly
            value={JSONBig.stringify(
              (explainResult as SemiJoinQueryExplanation).mainQuery.query,
              undefined,
              2,
            )}
          />
        </FormGroup>
        {(explainResult as SemiJoinQueryExplanation).mainQuery.signature && (
          <FormGroup label="Signature">
            <InputGroup
              defaultValue={(explainResult as SemiJoinQueryExplanation).mainQuery.signature || ''}
              readOnly
            />
          </FormGroup>
        )}
        <FormGroup label="Sub query">
          <TextArea
            readOnly
            value={JSONBig.stringify(
              (explainResult as SemiJoinQueryExplanation).subQueryRight.query,
              undefined,
              2,
            )}
          />
        </FormGroup>
        {(explainResult as SemiJoinQueryExplanation).subQueryRight.signature && (
          <FormGroup label="Signature">
            <InputGroup
              defaultValue={
                (explainResult as SemiJoinQueryExplanation).subQueryRight.signature || ''
              }
              readOnly
            />
          </FormGroup>
        )}
      </div>
    );
  } else {
    content = <div className="generic-result">{explainResult}</div>;
  }

  return (
    <Dialog className="explain-dialog" isOpen onClose={onClose} title="Query plan">
      <div className={Classes.DIALOG_BODY}>{content}</div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          {queryString && (
            <Button
              text="Open query"
              intent={Intent.PRIMARY}
              onClick={() => {
                if (queryString) setQueryString(queryString);
                onClose();
              }}
            />
          )}
        </div>
      </div>
    </Dialog>
  );
});
