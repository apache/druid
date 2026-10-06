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

import { Button, ButtonGroup, Intent } from '@blueprintjs/core';
import * as JSONBig from 'json-bigint-native';
import React from 'react';

import { Execution } from '../../druid-models';
import { hjson } from '../../editor-languages/hjson';
import { useQueryManager } from '../../hooks';
import { Api, AppToaster, UrlBaser } from '../../singletons';
import { copyToClipboard, downloadFile } from '../../utils';
import { ExecutionStagesPane } from '../../views/workbench-view/execution-stages-pane/execution-stages-pane';
import { CodeEditor } from '../code-editor/code-editor';
import { Loader } from '../loader/loader';

import './show-json-or-stages.scss';

export interface ShowJsonOrStagesProps {
  endpoint: string;
  transform?: (x: any) => any;
  downloadFilename?: string;
}

export const ShowJsonOrStages = React.memo(function ShowJsonOrStages(props: ShowJsonOrStagesProps) {
  const { endpoint, transform, downloadFilename } = props;

  const [jsonState, queryManager] = useQueryManager<null, [string, Execution | undefined]>({
    processQuery: async (_, signal) => {
      const resp = await Api.instance.get(endpoint, { signal });
      let data = resp.data;
      if (transform) data = transform(data);

      let execution: Execution | undefined;
      if (data.multiStageQuery) {
        try {
          execution = Execution.fromTaskReport(data);
        } catch (e) {
          console.error(`Could not parse task report as MSQ execution: ${e.message}`);
        }
      }

      return [
        typeof data === 'string' ? data : JSONBig.stringify(data, undefined, 2),
        execution,
      ] as [string, Execution | undefined];
    },
    initQuery: null,
  });

  const [jsonValue, execution] = jsonState.data || [''];
  return (
    <div className="show-json-or-stages">
      <div className="top-actions">
        <ButtonGroup className="right-buttons">
          <Button
            disabled={jsonState.loading}
            text="Refresh"
            variant="minimal"
            onClick={() => queryManager.rerunLastQuery()}
          />
          {downloadFilename && (
            <Button
              disabled={jsonState.loading}
              text="Download"
              variant="minimal"
              onClick={() => downloadFile(jsonValue, 'json', downloadFilename)}
            />
          )}
          <Button
            text="Copy"
            variant="minimal"
            disabled={jsonState.loading}
            onClick={() => {
              copyToClipboard(jsonValue);
              AppToaster.show({
                message: 'JSON value copied to clipboard',
                intent: Intent.SUCCESS,
              });
            }}
          />
          <Button
            text="View raw"
            disabled={!jsonValue}
            variant="minimal"
            onClick={() => window.open(UrlBaser.base(endpoint), '_blank')}
          />
        </ButtonGroup>
      </div>

      <div className="main-area">
        {jsonState.loading ? (
          <Loader />
        ) : execution ? (
          <ExecutionStagesPane execution={execution} />
        ) : (
          <CodeEditor
            language={hjson()}
            readOnly
            value={(!jsonState.error ? jsonValue : jsonState.getErrorMessage()) ?? ''}
          />
        )}
      </div>
    </div>
  );
});
