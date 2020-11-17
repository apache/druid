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

import axios from 'axios';
import { CancelToken } from 'axios';
import debounce from 'lodash.debounce';

import { QueryState } from './query-state';

export interface QueryManagerOptions<Q, R> {
  processQuery: (
    query: Q,
    cancelToken: CancelToken,
    setIntermediateQuery: (intermediateQuery: any) => void,
  ) => Promise<R>;
  onStateChange?: (queryResolve: QueryState<R>) => void;
  debounceIdle?: number;
  debounceLoading?: number;
}

export class QueryManager<Q, R> {
  private processQuery: (
    query: Q,
    cancelToken: CancelToken,
    setIntermediateQuery: (intermediateQuery: any) => void,
  ) => Promise<R>;
  private onStateChange?: (queryResolve: QueryState<R>) => void;

  private terminated = false;
  private nextQuery: Q | undefined;
  private lastQuery: Q | undefined;
  private lastIntermediateQuery: any;
  private currentRunCancelFn: (() => void) | undefined;
  private state: QueryState<R> = QueryState.INIT;
  private currentQueryId = 0;

  private runWhenIdle: () => void;
  private runWhenLoading: () => void;

  constructor(options: QueryManagerOptions<Q, R>) {
    this.processQuery = options.processQuery;
    this.onStateChange = options.onStateChange;
    if (options.debounceIdle !== 0) {
      this.runWhenIdle = debounce(this.run, options.debounceIdle || 100);
    } else {
      this.runWhenIdle = this.run;
    }
    if (options.debounceLoading !== 0) {
      this.runWhenLoading = debounce(this.run, options.debounceLoading || 200);
    } else {
      this.runWhenLoading = this.run;
    }
  }

  private setState(queryState: QueryState<R>) {
    this.state = queryState;
    if (this.onStateChange && !this.terminated) {
      this.onStateChange(queryState);
    }
  }

  private run() {
    this.lastQuery = this.nextQuery;
    if (typeof this.lastQuery === 'undefined') return;
    this.currentQueryId++;
    const myQueryId = this.currentQueryId;

    if (this.currentRunCancelFn) {
      this.currentRunCancelFn();
    }
    const cancelToken = new axios.CancelToken(cancelFn => {
      this.currentRunCancelFn = cancelFn;
    });
    this.processQuery(this.lastQuery, cancelToken, (intermediateQuery: any) => {
      this.lastIntermediateQuery = intermediateQuery;
    }).then(
      data => {
        if (this.currentQueryId !== myQueryId) return;
        this.currentRunCancelFn = undefined;
        this.setState(
          new QueryState<R>({
            data,
          }),
        );
      },
      (e: any) => {
        if (this.currentQueryId !== myQueryId) return;
        this.currentRunCancelFn = undefined;
        if (axios.isCancel(e)) {
          e = new Error(`canceled.`); // ToDo: fix!
        }
        this.setState(
          new QueryState<R>({
            error: e,
          }),
        );
      },
    );
  }

  private trigger() {
    const currentlyLoading = Boolean(this.currentRunCancelFn);

    this.setState(
      new QueryState<R>({
        loading: true,
      }),
    );

    if (currentlyLoading) {
      this.runWhenLoading();
    } else {
      this.runWhenIdle();
    }
  }

  public runQuery(query: Q): void {
    if (this.terminated) return;
    this.nextQuery = query;
    this.trigger();
  }

  public rerunLastQuery(runInBackground = false): void {
    if (this.terminated) return;
    this.nextQuery = this.lastQuery;
    if (runInBackground) {
      this.runWhenIdle();
    } else {
      this.trigger();
    }
  }

  public cancelCurrent(): void {
    if (!this.currentRunCancelFn) return;
    this.currentRunCancelFn();
    this.currentRunCancelFn = undefined;
  }

  public getLastQuery(): Q | undefined {
    return this.lastQuery;
  }

  public getLastIntermediateQuery(): any {
    return this.lastIntermediateQuery;
  }

  public getState(): QueryState<R> {
    return this.state;
  }

  public terminate(): void {
    this.terminated = true;
    if (this.currentRunCancelFn) {
      this.currentRunCancelFn();
    }
  }
}
