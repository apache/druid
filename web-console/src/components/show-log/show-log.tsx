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

import { AnchorButton, Button, ButtonGroup, Checkbox, Intent } from '@blueprintjs/core';
import axios from 'axios';
import copy from 'copy-to-clipboard';
import React from 'react';

import { Loader } from '../../components';
import { AppToaster } from '../../singletons/toaster';
import { UrlBaser } from '../../singletons/url-baser';
import { QueryManager } from '../../utils';

import './show-log.scss';

function removeFirstPartialLine(log: string): string {
  const lines = log.split('\n');
  if (lines.length > 1) {
    lines.shift();
  }
  return lines.join('\n');
}

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
  static CHECK_INTERVAL = 2500;

  private showLogQueryManager: QueryManager<null, string>;
  private log = React.createRef<HTMLTextAreaElement>();
  private interval: number | undefined;

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
        const { tail } = this.state;
        if (result && tail) {
          const { current } = this.log;
          if (current) {
            current.scrollTop = current.scrollHeight;
          }
        }
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
      this.addTailer();
    }

    this.showLogQueryManager.runQuery(null);
  }

  componentWillUnmount(): void {
    this.removeTailer();
  }

  addTailer() {
    if (this.interval) return;
    this.interval = Number(
      setInterval(() => this.showLogQueryManager.rerunLastQuery(true), ShowLog.CHECK_INTERVAL),
    );
  }

  removeTailer() {
    if (!this.interval) return;
    clearInterval(this.interval);
    delete this.interval;
  }

  private handleCheckboxChange = () => {
    const { tail } = this.state;

    const nextTail = !tail;
    this.setState({
      tail: nextTail,
    });
    if (nextTail) {
      this.addTailer();
    } else {
      this.removeTailer();
    }
  };

  render(): JSX.Element {
    const { endpoint, downloadFilename, status } = this.props;
    const { logValue, error, loading } = this.state;

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
              <AnchorButton
                text="Save"
                minimal
                download={downloadFilename}
                href={UrlBaser.base(endpoint)}
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
          {loading ? (
            <Loader loadingText="" loading />
          ) : (
            <textarea
              className="bp3-input"
              readOnly
              value={logValue ? logValue : error}
              ref={this.log}
            />
          )}
        </div>
      </div>
    );
  }
}
