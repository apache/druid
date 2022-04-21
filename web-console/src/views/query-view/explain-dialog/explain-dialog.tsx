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
  Tab,
  Tabs,
  TextArea,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import * as JSONBig from 'json-bigint-native';
import React from 'react';

import { Loader } from '../../../components';
import { useQueryManager } from '../../../hooks';
import {
  formatSignature,
  getDruidErrorMessage,
  queryDruidSql,
  QueryExplanation,
  QueryWithContext,
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
  return `EXPLAIN PLAN FOR ${query}`;
}

export interface ExplainDialogProps {
  queryWithContext: QueryWithContext;
  mandatoryQueryContext?: Record<string, any>;
  onClose: () => void;
  setQueryString: (queryString: string) => void;
}

export const ExplainDialog = React.memo(function ExplainDialog(props: ExplainDialogProps) {
  const { queryWithContext, onClose, setQueryString, mandatoryQueryContext } = props;

  const [explainState] = useQueryManager<QueryWithContext, QueryExplanation[] | string>({
    processQuery: async (queryWithContext: QueryWithContext) => {
      const { queryString, queryContext, wrapQueryLimit } = queryWithContext;

      let context: Record<string, any> | undefined;
      if (!isEmptyContext(queryContext) || wrapQueryLimit || mandatoryQueryContext) {
        context = {
          ...queryContext,
          ...(mandatoryQueryContext || {}),
          useNativeQueryExplain: true,
        };
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

      const plan = result[0]['PLAN'];
      if (typeof plan !== 'string') {
        throw new Error(`unexpected result from server`);
      }

      try {
        return JSONBig.parse(plan);
      } catch {
        return plan;
      }
    },
    initQuery: queryWithContext,
  });

  let content: JSX.Element;

  const { loading, error: explainError, data: explainResult } = explainState;

  function renderQueryExplanation(queryExplanation: QueryExplanation) {
    const queryString = JSONBig.stringify(queryExplanation.query, undefined, 2);
    return (
      <div className="query-explanation">
        <FormGroup className="query-group" label="Query">
          <TextArea readOnly value={queryString} />
        </FormGroup>
        <FormGroup className="signature-group" label="Signature">
          <InputGroup defaultValue={formatSignature(queryExplanation)} readOnly />
        </FormGroup>
        <Button
          className="open-query"
          text="Open query"
          rightIcon={IconNames.ARROW_TOP_RIGHT}
          intent={Intent.PRIMARY}
          minimal
          onClick={() => {
            setQueryString(queryString);
            onClose();
          }}
        />
      </div>
    );
  }

  if (loading) {
    content = <Loader />;
  } else if (explainError) {
    content = <div>{explainError.message}</div>;
  } else if (!explainResult) {
    content = <div />;
  } else if (Array.isArray(explainResult) && explainResult.length) {
    if (explainResult.length === 1) {
      content = renderQueryExplanation(explainResult[0]);
    } else {
      content = (
        <Tabs animate renderActiveTabPanelOnly vertical>
          {explainResult.map((queryExplanation, i) => (
            <Tab
              id={i}
              key={i}
              title={`Query ${i + 1}`}
              panel={renderQueryExplanation(queryExplanation)}
            />
          ))}
          <Tabs.Expander />
        </Tabs>
      );
    }
  } else {
    content = <div className="generic-result">{explainResult}</div>;
  }

  return (
    <Dialog className="explain-dialog" isOpen onClose={onClose} title="Query plan">
      <div className={Classes.DIALOG_BODY}>{content}</div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
        </div>
      </div>
    </Dialog>
  );
});
