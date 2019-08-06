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

import debounce from 'lodash.debounce';

export interface QueryStateInt<R> {
  result?: R;
  loading: boolean;
  error?: string;
}

export interface QueryManagerOptions<Q, R> {
  processQuery: (query: Q, setIntermediateQuery: (intermediateQuery: any) => void) => Promise<R>;
  onStateChange?: (queryResolve: QueryStateInt<R>) => void;
  debounceIdle?: number;
  debounceLoading?: number;
}

export class QueryManager<Q, R> {
  private processQuery: (
    query: Q,
    setIntermediateQuery: (intermediateQuery: any) => void,
  ) => Promise<R>;
  private onStateChange?: (queryResolve: QueryStateInt<R>) => void;

  private terminated = false;
  private nextQuery: Q | undefined;
  private lastQuery: Q | undefined;
  private lastIntermediateQuery: any;
  private actuallyLoading = false;
  private state: QueryStateInt<R> = {
    loading: false,
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
    if (typeof this.lastQuery === 'undefined') return;
    this.currentQueryId++;
    const myQueryId = this.currentQueryId;

    this.actuallyLoading = true;
    this.processQuery(this.lastQuery, (intermediateQuery: any) => {
      this.lastIntermediateQuery = intermediateQuery;
    }).then(
      result => {
        if (this.currentQueryId !== myQueryId) return;
        this.actuallyLoading = false;
        this.setState({
          result,
          loading: false,
          error: undefined,
        });
      },
      (e: Error) => {
        if (this.currentQueryId !== myQueryId) return;
        this.actuallyLoading = false;
        this.setState({
          result: undefined,
          loading: false,
          error: e.message,
        });
      },
    );
  }

  private trigger() {
    const currentActuallyLoading = this.actuallyLoading;

    this.setState({
      result: undefined,
      loading: true,
      error: undefined,
    });

    if (currentActuallyLoading) {
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

  public getLastQuery(): Q | undefined {
    return this.lastQuery;
  }

  public getLastIntermediateQuery(): any {
    return this.lastIntermediateQuery;
  }

  public getState(): QueryStateInt<R> {
    return this.state;
  }

  public terminate(): void {
    this.terminated = true;
  }
}
