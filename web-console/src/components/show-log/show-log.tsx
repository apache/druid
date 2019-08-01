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

import { Button, ButtonGroup, Checkbox, Intent } from '@blueprintjs/core';
import axios from 'axios';
import copy from 'copy-to-clipboard';
import React from 'react';

import { AppToaster } from '../../singletons/toaster';
import { UrlBaser } from '../../singletons/url-baser';
import { downloadFile, QueryManager } from '../../utils';

import './show-log.scss';

function removeFirstPartialLine(log: string): string {
  const lines = log.split('\n');
  if (lines.length > 1) {
    lines.shift();
  }
  return lines.join('\n');
}

let interval: number | undefined;

export interface ShowLogProps {
  endpoint: string;
  downloadFilename?: string;
  tailOffset?: number;
  status?: string;
}

export interface ShowLogState {
  logValue?: string;
  loading: boolean;
  error?: string;
  tail: boolean;
}

export class ShowLog extends React.PureComponent<ShowLogProps, ShowLogState> {
  private showLogQueryManager: QueryManager<null, string>;
  public log = React.createRef<HTMLTextAreaElement>();

  constructor(props: ShowLogProps, context: any) {
    super(props, context);
    this.state = {
      tail: true,
      loading: true,
    };

    this.showLogQueryManager = new QueryManager({
      processQuery: async () => {
        const { endpoint, tailOffset } = this.props;
        const resp = await axios.get(endpoint + (tailOffset ? `?offset=-${tailOffset}` : ''));
        const data = resp.data;

        let logValue = typeof data === 'string' ? data : JSON.stringify(data, undefined, 2);
        if (tailOffset) logValue = removeFirstPartialLine(logValue);
        return logValue;
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          logValue: result,
          loading,
          error,
        });
      },
    });
  }

  componentDidMount(): void {
    const { status } = this.props;

    if (status === 'RUNNING') {
      interval = Number(setInterval(() => this.showLogQueryManager.runQuery(null), 2000));
    }

    this.showLogQueryManager.runQuery(null);
  }

  componentWillUnmount(): void {
    clearInterval(interval);
  }

  private handleCheckboxChange = () => {
    this.setState({
      tail: !this.state.tail,
    });
    if (!this.state.tail) {
      interval = Number(setInterval(() => this.showLogQueryManager.runQuery(null), 2000));
    } else {
      clearInterval(interval);
    }
  };

  render(): JSX.Element {
    const { endpoint, downloadFilename, status } = this.props;
    const { logValue, error } = this.state;

    return (
      <div className="show-log">
        <div className="top-actions">
          {status === 'RUNNING' && (
            <Checkbox
              label="Tail log"
              checked={this.state.tail}
              onChange={this.handleCheckboxChange}
            />
          )}
          <ButtonGroup className="right-buttons">
            {downloadFilename && (
              <Button
                text="Save"
                minimal
                onClick={() => downloadFile(logValue ? logValue : '', 'plain', downloadFilename)}
              />
            )}
            <Button
              text="Copy"
              minimal
              onClick={() => {
                copy(logValue ? logValue : '', { format: 'text/plain' });
                AppToaster.show({
                  message: 'Log copied to clipboard',
                  intent: Intent.SUCCESS,
                });
              }}
            />
            <Button
              text="View full log"
              minimal
              onClick={() => window.open(UrlBaser.base(endpoint), '_blank')}
            />
          </ButtonGroup>
        </div>
        <div className="main-area">
          <textarea
            className="bp3-input"
            readOnly
            value={logValue ? logValue : error}
            ref={this.log}
          />
        </div>
      </div>
    );
  }
}
