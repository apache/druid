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

import { Button, ButtonGroup, InputGroup, Intent, TextArea } from '@blueprintjs/core';
import axios from 'axios';
import * as React from 'react';
import * as CopyToClipboard from 'react-copy-to-clipboard';

import { AppToaster } from '../singletons/toaster';
import { UrlBaser } from '../singletons/url-baser';
import { downloadFile } from '../utils';

import './show-json.scss';

export interface ShowJsonProps extends React.Props<any> {
  endpoint: string;
  downloadFilename?: string;
}

export interface ShowJsonState {
  jsonValue: string;
}

export class ShowJson extends React.Component<ShowJsonProps, ShowJsonState> {
  constructor(props: ShowJsonProps, context: any) {
    super(props, context);
    this.state = {
      jsonValue: ''
    };

    this.getJsonInfo();
  }

  private getJsonInfo = async (): Promise<void> => {
    const { endpoint } = this.props;
    try {
      const resp = await axios.get(endpoint);
      const data = resp.data;
      this.setState({
        jsonValue: typeof (data) === 'string' ? data : JSON.stringify(data, undefined, 2)
      });
    } catch (e) {
      this.setState({
        jsonValue: `Error: ` + e.response.data
      });
    }
  }

  render() {
    const { endpoint, downloadFilename } = this.props;
    const { jsonValue } = this.state;

    return <div className="show-json">
      <div className="top-actions">
        <ButtonGroup className="right-buttons">
          {
            downloadFilename &&
            <Button
              text="Save"
              minimal
              onClick={() => downloadFile(jsonValue, 'json', downloadFilename)}
            />
          }
          <CopyToClipboard text={jsonValue}>
            <Button
              text="Copy"
              minimal
              onClick={() => {
                AppToaster.show({
                  message: 'Copied JSON to clipboard',
                  intent: Intent.SUCCESS
                });
              }}
            />
          </CopyToClipboard>
          <Button
            text="View raw"
            minimal
            onClick={() => window.open(UrlBaser.base(endpoint), '_blank')}
          />
        </ButtonGroup>
      </div>
      <div className="main-area">
        <TextArea
          readOnly
          value={jsonValue}
        />
      </div>
    </div>;
  }
}
