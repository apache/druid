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

import { AnchorButton, Button, ButtonGroup, Intent, Switch } from '@blueprintjs/core';
import copy from 'copy-to-clipboard';
import * as JSONBig from 'json-bigint-native';
import React from 'react';

import { Loader } from '../../components';
import { Api, AppToaster, UrlBaser } from '../../singletons';
import { QueryManager, QueryState } from '../../utils';

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
  logState: QueryState<string>;
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
      logState: QueryState.INIT,
      tail: true,
    };

    this.showLogQueryManager = new QueryManager({
      processQuery: async () => {
        const { endpoint, tailOffset } = this.props;
        const resp = await Api.instance.get(
          endpoint + (tailOffset ? `?offset=-${tailOffset}` : ''),
        );
        const data = resp.data;

        let logValue = typeof data === 'string' ? data : JSONBig.stringify(data, undefined, 2);
        if (tailOffset) logValue = removeFirstPartialLine(logValue);
        return logValue;
      },
      onStateChange: logState => {
        if (logState.data) {
          this.scrollToBottomIfNeeded();
        }
        this.setState({
          logState,
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
    this.showLogQueryManager.terminate();
    this.removeTailer();
  }

  private scrollToBottomIfNeeded(): void {
    const { tail } = this.state;
    if (!tail) return;

    const { current } = this.log;
    if (current) {
      current.scrollTop = current.scrollHeight;
    }
  }

  addTailer() {
    if (this.interval) return;
    this.interval = setInterval(
      () => this.showLogQueryManager.rerunLastQuery(true),
      ShowLog.CHECK_INTERVAL,
    ) as any;
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
    const { logState } = this.state;

    return (
      <div className="show-log">
        <div className="top-actions">
          {status === 'RUNNING' && (
            <Switch
              className="tail-log"
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
                copy(logState.data || '', { format: 'text/plain' });
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
          {logState.loading ? (
            <Loader />
          ) : (
            <textarea
              className="bp3-input"
              readOnly
              value={logState.data || logState.getErrorMessage()}
              ref={this.log}
            />
          )}
        </div>
      </div>
    );
  }
}
