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
import { downloadFile } from '../../utils';

import './show-json.scss';

export interface ShowJsonProps {
  endpoint: string;
  transform?: (x: any) => any;
  downloadFilename?: string;
}

export interface ShowJsonState {
  jsonValue: string;
}

export class ShowJson extends React.PureComponent<ShowJsonProps, ShowJsonState> {
  constructor(props: ShowJsonProps, context: any) {
    super(props, context);
    this.state = {
      jsonValue: '',
    };

    this.getJsonInfo();
  }

  private getJsonInfo = async (): Promise<void> => {
    const { endpoint, transform } = this.props;

    try {
      const resp = await axios.get(endpoint);
      let data = resp.data;
      if (transform) data = transform(data);
      this.setState({
        jsonValue: typeof data === 'string' ? data : JSON.stringify(data, undefined, 2),
      });
    } catch (e) {
      this.setState({
        jsonValue: `Error: ` + e.response.data,
      });
    }
  };

  render(): JSX.Element {
    const { endpoint, downloadFilename } = this.props;
    const { jsonValue } = this.state;

    return (
      <div className="show-json">
        <div className="top-actions">
          <ButtonGroup className="right-buttons">
            {downloadFilename && (
              <Button
                text="Save"
                minimal
                onClick={() => downloadFile(jsonValue, 'json', downloadFilename)}
              />
            )}
            <Button
              text="Copy"
              minimal
              onClick={() => {
                copy(jsonValue, { format: 'text/plain' });
                AppToaster.show({
                  message: 'JSON copied to clipboard',
                  intent: Intent.SUCCESS,
                });
              }}
            />
            <Button
              text="View raw"
              minimal
              onClick={() => window.open(UrlBaser.base(endpoint), '_blank')}
            />
          </ButtonGroup>
        </div>
        <div className="main-area">
          <TextArea readOnly value={jsonValue} />
        </div>
      </div>
    );
  }
}
