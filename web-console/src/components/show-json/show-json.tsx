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
import axios from 'axios';
import copy from 'copy-to-clipboard';
import React from 'react';

import { AppToaster } from '../../singletons/toaster';
import { UrlBaser } from '../../singletons/url-baser';
import { downloadFile, QueryManager } from '../../utils';
import { Loader } from '../loader/loader';

import './show-json.scss';

export interface ShowJsonProps {
  endpoint: string;
  transform?: (x: any) => any;
  downloadFilename?: string;
}

export interface ShowJsonState {
  jsonValue?: string;
  loading: boolean;
  error?: string;
}

export class ShowJson extends React.PureComponent<ShowJsonProps, ShowJsonState> {
  private showJsonQueryManager: QueryManager<null, string>;
  constructor(props: ShowJsonProps, context: any) {
    super(props, context);
    this.state = {
      jsonValue: '',
      loading: false,
    };
    this.showJsonQueryManager = new QueryManager({
      processQuery: async () => {
        const { endpoint, transform } = this.props;
        const resp = await axios.get(endpoint);
        let data = resp.data;
        if (transform) data = transform(data);
        return typeof data === 'string' ? data : JSON.stringify(data, undefined, 2);
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          loading,
          error,
          jsonValue: result,
        });
      },
    });
  }

  componentDidMount(): void {
    this.showJsonQueryManager.runQuery(null);
  }

  render(): JSX.Element {
    const { endpoint, downloadFilename } = this.props;
    const { jsonValue, error, loading } = this.state;

    return (
      <div className="show-json">
        <div className="top-actions">
          <ButtonGroup className="right-buttons">
            {downloadFilename && (
              <Button
                disabled={loading}
                text="Save"
                minimal
                onClick={() => downloadFile(jsonValue ? jsonValue : '', 'json', downloadFilename)}
              />
            )}
            <Button
              text="Copy"
              minimal
              disabled={loading}
              onClick={() => {
                copy(jsonValue ? jsonValue : '', { format: 'text/plain' });
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
          {loading ? <Loader /> : <TextArea readOnly value={!error ? jsonValue : error} />}
        </div>
      </div>
    );
  }
}
