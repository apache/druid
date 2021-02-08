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

import { Button, ButtonGroup, Intent, TextArea } from '@blueprintjs/core';
import copy from 'copy-to-clipboard';
import * as JSONBig from 'json-bigint-native';
import React from 'react';

import { useQueryManager } from '../../hooks';
import { Api, AppToaster, UrlBaser } from '../../singletons';
import { downloadFile } from '../../utils';
import { Loader } from '../loader/loader';

import './show-json.scss';

export interface ShowJsonProps {
  endpoint: string;
  transform?: (x: any) => any;
  downloadFilename?: string;
}

export const ShowJson = React.memo(function ShowJson(props: ShowJsonProps) {
  const { endpoint, transform, downloadFilename } = props;

  const [jsonState] = useQueryManager<null, string>({
    processQuery: async () => {
      const resp = await Api.instance.get(endpoint);
      let data = resp.data;
      if (transform) data = transform(data);
      return typeof data === 'string' ? data : JSONBig.stringify(data, undefined, 2);
    },
    initQuery: null,
  });

  const jsonValue = jsonState.data || '';
  return (
    <div className="show-json">
      <div className="top-actions">
        <ButtonGroup className="right-buttons">
          {downloadFilename && (
            <Button
              disabled={jsonState.loading}
              text="Save"
              minimal
              onClick={() => downloadFile(jsonValue, 'json', downloadFilename)}
            />
          )}
          <Button
            text="Copy"
            minimal
            disabled={jsonState.loading}
            onClick={() => {
              copy(jsonValue, { format: 'text/plain' });
              AppToaster.show({
                message: 'JSON value copied to clipboard',
                intent: Intent.SUCCESS,
              });
            }}
          />
          <Button
            text="View raw"
            disabled={!jsonValue}
            minimal
            onClick={() => window.open(UrlBaser.base(endpoint), '_blank')}
          />
        </ButtonGroup>
      </div>
      <div className="main-area">
        {jsonState.loading ? (
          <Loader />
        ) : (
          <TextArea readOnly value={!jsonState.error ? jsonValue : jsonState.getErrorMessage()} />
        )}
      </div>
    </div>
  );
});
