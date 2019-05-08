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

import debounce = require('lodash.debounce');

export interface QueryStateInt<R> {
  result: R | null;
  loading: boolean;
  error: string | null;
}

export interface QueryManagerOptions<Q, R> {
  processQuery: (query: Q) => Promise<R>;
  onStateChange?: (queryResolve: QueryStateInt<R>) => void;
  debounceIdle?: number;
  debounceLoading?: number;
}

export class QueryManager<Q, R> {
  private processQuery: (query: Q) => Promise<R>;
  private onStateChange?: (queryResolve: QueryStateInt<R>) => void;

  private terminated = false;
  private nextQuery: Q;
  private lastQuery: Q;
  private actuallyLoading = false;
  private state: QueryStateInt<R> = {
    result: null,
    loading: false,
    error: null
  };
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

  private setState(queryState: QueryStateInt<R>) {
    this.state = queryState;
    if (this.onStateChange && !this.terminated) {
      this.onStateChange(queryState);
    }
  }

  private run() {
    this.lastQuery = this.nextQuery;
    this.currentQueryId++;
    const myQueryId = this.currentQueryId;

    this.actuallyLoading = true;
    this.processQuery(this.lastQuery)
      .then(
        (result) => {
          if (this.currentQueryId !== myQueryId) return;
          this.actuallyLoading = false;
          this.setState({
            result,
            loading: false,
            error: null
          });
        },
        (e: Error) => {
          if (this.currentQueryId !== myQueryId) return;
          this.actuallyLoading = false;
          this.setState({
            result: null,
            loading: false,
            error: e.message
          });
        }
      );
  }

  private trigger() {
    const currentActuallyLoading = this.actuallyLoading;

    this.setState({
      result: null,
      loading: true,
      error: null
    });

    if (currentActuallyLoading) {
      this.runWhenLoading();
    } else {
      this.runWhenIdle();
    }
  }

  public runQuery(query: Q): void {
    this.nextQuery = query;
    this.trigger();
  }

  public rerunLastQuery(): void {
    this.nextQuery = this.lastQuery;
    this.trigger();
  }

  public getLastQuery(): Q {
    return this.lastQuery;
  }

  public getState(): QueryStateInt<R> {
    return this.state;
  }

  public terminate(): void {
    this.terminated = true;
  }
}
