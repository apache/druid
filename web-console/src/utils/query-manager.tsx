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

import axios, { Canceler, CancelToken } from 'axios';
import debounce from 'lodash.debounce';

import { wait } from './general';
import { IntermediateQueryState } from './intermediate-query-state';
import { QueryState } from './query-state';

export interface QueryManagerOptions<Q, R, I = never, E extends Error = Error> {
  initState?: QueryState<R, E, I>;
  processQuery: (
    query: Q,
    cancelToken: CancelToken,
    setIntermediateQuery: (intermediateQuery: any) => void,
  ) => Promise<R | IntermediateQueryState<I>>;
  backgroundStatusCheck?: (
    state: I,
    query: Q,
    cancelToken: CancelToken,
  ) => Promise<R | IntermediateQueryState<I>>;
  onStateChange?: (queryResolve: QueryState<R, E, I>) => void;
  debounceIdle?: number;
  debounceLoading?: number;
  backgroundStatusCheckInitDelay?: number;
  backgroundStatusCheckDelay?: number;
  swallowBackgroundError?: (e: Error) => boolean;
}

export class QueryManager<Q, R, I = never, E extends Error = Error> {
  static TERMINATION_MESSAGE = 'QUERY_MANAGER_TERMINATED';

  private readonly processQuery: (
    query: Q,
    cancelToken: CancelToken,
    setIntermediateQuery: (intermediateQuery: any) => void,
  ) => Promise<R | IntermediateQueryState<I>>;

  private readonly backgroundStatusCheck?: (
    state: I,
    query: Q,
    cancelToken: CancelToken,
  ) => Promise<R | IntermediateQueryState<I>>;

  private readonly onStateChange?: (queryResolve: QueryState<R, E, I>) => void;
  private readonly backgroundStatusCheckInitDelay: number;
  private readonly backgroundStatusCheckDelay: number;
  private readonly swallowBackgroundError?: (e: Error) => boolean;

  private terminated = false;
  private nextQuery: Q | undefined;
  private lastQuery: Q | undefined;
  private lastIntermediateQuery: any;
  private currentRunCancelFn?: Canceler;
  private state: QueryState<R, E, I>;
  private currentQueryId = 0;

  private readonly runWhenIdle: () => void;
  private readonly runWhenLoading: () => void;

  constructor(options: QueryManagerOptions<Q, R, I, E>) {
    this.processQuery = options.processQuery;
    this.backgroundStatusCheck = options.backgroundStatusCheck;
    this.onStateChange = options.onStateChange;
    this.backgroundStatusCheckInitDelay = options.backgroundStatusCheckInitDelay || 500;
    this.backgroundStatusCheckDelay = options.backgroundStatusCheckDelay || 1000;
    this.swallowBackgroundError = options.swallowBackgroundError;
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
    this.state = options.initState || QueryState.INIT;
  }

  private setState(queryState: QueryState<R, E, I>) {
    this.state = queryState;
    if (this.onStateChange && !this.terminated) {
      this.onStateChange(queryState);
    }
  }

  private async run(): Promise<void> {
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

    const query = this.lastQuery;
    let data: R | IntermediateQueryState<I>;
    try {
      data = await this.processQuery(query, cancelToken, (intermediateQuery: any) => {
        this.lastIntermediateQuery = intermediateQuery;
      });
    } catch (e) {
      if (this.currentQueryId !== myQueryId) return;
      this.currentRunCancelFn = undefined;
      this.setState(
        new QueryState<R, E>({
          error: axios.isCancel(e) ? new Error(`canceled.`) : e, // remap cancellation into a simple error to hide away the axios implementation specifics
          lastData: this.state.getSomeData(),
        }),
      );
      return;
    }

    let backgroundChecks = 0;
    let intermediateError: Error | undefined;
    while (data instanceof IntermediateQueryState) {
      try {
        if (!this.backgroundStatusCheck) {
          throw new Error(
            'backgroundStatusCheck must be set if intermediate query state is returned',
          );
        }
        cancelToken.throwIfRequested();

        this.setState(
          new QueryState<R, E, I>({
            loading: true,
            intermediate: data.state,
            intermediateError,
            lastData: this.state.getSomeData(),
          }),
        );

        const delay =
          data.delay ??
          (backgroundChecks > 0
            ? this.backgroundStatusCheckDelay
            : this.backgroundStatusCheckInitDelay);

        if (delay) {
          await wait(delay);
          cancelToken.throwIfRequested();
        }

        data = await this.backgroundStatusCheck(data.state, query, cancelToken);
        intermediateError = undefined; // Clear the intermediate error if there was one
      } catch (e) {
        if (this.currentQueryId !== myQueryId) return;
        if (this.swallowBackgroundError?.(e)) {
          intermediateError = e;
        } else {
          this.currentRunCancelFn = undefined;
          this.setState(
            new QueryState<R, E>({
              error: axios.isCancel(e) ? new Error(`canceled.`) : e, // remap cancellation into a simple error to hide away the axios implementation specifics
              lastData: this.state.getSomeData(),
            }),
          );
          return;
        }
      }

      backgroundChecks++;
    }

    if (this.currentQueryId !== myQueryId) return;
    this.currentRunCancelFn = undefined;
    this.setState(
      new QueryState<R, E>({
        data,
        lastData: this.state.getSomeData(),
      }),
    );
  }

  private trigger() {
    if (this.currentRunCancelFn) {
      // Currently loading
      this.runWhenLoading();
    } else {
      this.setState(
        new QueryState<R, E>({
          loading: true,
          lastData: this.state.getSomeData(),
        }),
      );

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

  public getState(): QueryState<R, Error, I> {
    return this.state;
  }

  public reset(): void {
    this.cancelCurrent();
    this.setState(QueryState.INIT);
  }

  public terminate(): void {
    this.terminated = true;
    if (this.currentRunCancelFn) {
      this.currentRunCancelFn(QueryManager.TERMINATION_MESSAGE);
    }
  }

  public isTerminated(): boolean {
    return this.terminated;
  }
}
