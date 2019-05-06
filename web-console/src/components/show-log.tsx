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

import './show-log.scss';

function removeFirstPartialLine(log: string): string {
  const lines = log.split('\n');
  if (lines.length > 1) {
    lines.shift();
  }
  return lines.join('\n');
}

export interface ShowLogProps extends React.Props<any> {
  endpoint: string;
  downloadFilename?: string;
  tailOffset?: number;
}

export interface ShowLogState {
  logValue: string;
}

export class ShowLog extends React.Component<ShowLogProps, ShowLogState> {
  constructor(props: ShowLogProps, context: any) {
    super(props, context);
    this.state = {
      logValue: ''
    };

    this.getLogInfo();
  }

  private getLogInfo = async (): Promise<void> => {
    const { endpoint, tailOffset } = this.props;
    try {
      const resp = await axios.get(endpoint + (tailOffset ? `?offset=-${tailOffset}` : ''));
      const data = resp.data;

      let logValue = typeof (data) === 'string' ? data : JSON.stringify(data, undefined, 2);
      if (tailOffset) logValue = removeFirstPartialLine(logValue);
      this.setState({ logValue });
    } catch (e) {
      this.setState({
        logValue: `Error: ` + e.response.data
      });
    }
  }

  render() {
    const { endpoint, downloadFilename } = this.props;
    const { logValue } = this.state;

    return <div className="show-log">
      <div className="top-actions">
        <ButtonGroup className="right-buttons">
          {
            downloadFilename &&
            <Button
              text="Save"
              minimal
              onClick={() => downloadFile(logValue, 'plain', downloadFilename)}
            />
          }
          <CopyToClipboard text={logValue}>
            <Button
              text="Copy"
              minimal
              onClick={() => {
                AppToaster.show({
                  message: 'Copied log to clipboard',
                  intent: Intent.SUCCESS
                });
              }}
            />
          </CopyToClipboard>
          <Button
            text="View full log"
            minimal
            onClick={() => window.open(UrlBaser.base(endpoint), '_blank')}
          />
        </ButtonGroup>
      </div>
      <div className="main-area">
        <TextArea
          readOnly
          value={logValue}
        />
      </div>
    </div>;
  }
}
