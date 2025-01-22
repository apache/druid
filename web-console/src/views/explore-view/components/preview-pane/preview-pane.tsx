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

import { Callout } from '@blueprintjs/core';
import classNames from 'classnames';
import type { QueryResult, SqlQuery } from 'druid-query-toolkit';
import React from 'react';

import { useQueryManager } from '../../../../hooks';
import { formatEmpty } from '../../../../utils';

import './preview-pane.scss';

function getPreviewValues(queryResult: QueryResult): any[] {
  const headerNames = queryResult.getHeaderNames();
  if (headerNames.length === 1) {
    return queryResult.getColumnByName(headerNames[0]) || [];
  } else {
    return queryResult.rows[0] || [];
  }
}

export interface PreviewPaneProps {
  previewQuery: string | undefined;
  runSqlQuery(query: string | SqlQuery): Promise<QueryResult>;
}

export const PreviewPane = React.memo(function PreviewPane(props: PreviewPaneProps) {
  const { previewQuery, runSqlQuery } = props;

  const [previewState] = useQueryManager({
    query: previewQuery,
    processQuery: runSqlQuery,
    debounceIdle: 1000,
  });

  const previewValues = previewState.data ? getPreviewValues(previewState.data) : undefined;
  return (
    <Callout className="preview-pane" title="Preview">
      {previewState.loading && 'Loading...'}
      {previewState.error && <div className="preview-error">{previewState.getErrorMessage()}</div>}
      {previewValues &&
        (previewValues.length ? (
          <div className="preview-values-wrapper">
            <div className="preview-values">
              {previewValues.map((v, i) => (
                <div
                  className={classNames('preview-value', { special: v == null || v === '' })}
                  key={i}
                >
                  {formatEmpty(v)}
                </div>
              ))}
            </div>
          </div>
        ) : (
          'No preview values'
        ))}
    </Callout>
  );
});
