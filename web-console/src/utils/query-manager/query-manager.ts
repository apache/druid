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
import debounce from 'lodash.debounce';

import { wait } from '../general';

import { IntermediateQueryState } from './intermediate-query-state';
import { QueryState } from './query-state';
import { ResultWithAuxiliaryWork } from './result-with-auxiliary-work';

export interface ProcessQueryExtra<I = never> {
  setIntermediateQuery: (intermediateQuery: any) => void;
  setIntermediateStateCallback: (intermediateStateCallback: () => Promise<I>) => void;
}

export interface QueryManagerOptions<Q, R, I = never, E extends Error = Error> {
  initState?: QueryState<R, E, I>;
  processQuery: (
    query: Q,
    signal: AbortSignal,
    extra: ProcessQueryExtra<I>,
  ) => Promise<R | IntermediateQueryState<I> | ResultWithAuxiliaryWork<R>>;
  backgroundStatusCheck?: (
    state: I,
    query: Q,
    signal: AbortSignal,
  ) => Promise<R | IntermediateQueryState<I> | ResultWithAuxiliaryWork<R>>;
  onStateChange?: (queryResolve: QueryState<R, E, I>) => void;
  debounceInit?: number;
  debounceIdle?: number;
  debounceLoading?: number;
  backgroundStatusCheckInitDelay?: number;
  backgroundStatusCheckDelay?: number;
  swallowBackgroundError?: (e: Error) => boolean;
}

export class QueryManager<Q, R, I = never, E extends Error = Error> {
  static TERMINATION_MESSAGE = 'QUERY_MANAGER_TERMINATED';

  static remapAxiosCancellationIntoError(e: any) {
    return axios.isCancel(e) ? new Error(e.message ?? 'Browser request canceled') : e;
  }

  private readonly processQuery: (
    query: Q,
    signal: AbortSignal,
    extra: ProcessQueryExtra<I>,
  ) => Promise<R | IntermediateQueryState<I> | ResultWithAuxiliaryWork<R>>;

  private readonly backgroundStatusCheck?: (
    state: I,
    query: Q,
    signal: AbortSignal,
  ) => Promise<R | IntermediateQueryState<I> | ResultWithAuxiliaryWork<R>>;

  private readonly onStateChange?: (queryResolve: QueryState<R, E, I>) => void;
  private readonly backgroundStatusCheckInitDelay: number;
  private readonly backgroundStatusCheckDelay: number;
  private readonly swallowBackgroundError?: (e: Error) => boolean;

  private terminated = false;
  private nextQuery: Q | undefined;
  private lastQuery: Q | undefined;
  private lastIntermediateQuery: any;
  private currentRunCancelFn?: (reason?: string) => void;
  private state: QueryState<R, E, I>;
  private currentQueryId = 0;

  private readonly runWhenInit: () => void | Promise<void>;
  private readonly runWhenIdle: () => void | Promise<void>;
  private readonly runWhenLoading: () => void | Promise<void>;

  constructor(options: QueryManagerOptions<Q, R, I, E>) {
    this.processQuery = options.processQuery;
    this.backgroundStatusCheck = options.backgroundStatusCheck;
    this.onStateChange = options.onStateChange;
    this.backgroundStatusCheckInitDelay = options.backgroundStatusCheckInitDelay || 500;
    this.backgroundStatusCheckDelay = options.backgroundStatusCheckDelay || 1000;
    this.swallowBackgroundError = options.swallowBackgroundError;
    if (options.debounceInit !== 0) {
      this.runWhenInit = debounce(this.run, options.debounceInit || 50);
    } else {
      this.runWhenInit = this.run;
    }
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
    const controller = new AbortController();
    this.currentRunCancelFn = (reason: string | undefined) => controller.abort(reason);
    const signal = controller.signal;

    const query = this.lastQuery;
    let data: R | IntermediateQueryState<I> | ResultWithAuxiliaryWork<R>;
    try {
      data = await this.processQuery(query, signal, {
        setIntermediateQuery: (intermediateQuery: any) => {
          this.lastIntermediateQuery = intermediateQuery;
        },
        setIntermediateStateCallback: intermediateStateCallback => {
          let backgroundChecks = 0;
          let intermediateError: Error | undefined;

          void (async () => {
            while (!signal.aborted && this.currentQueryId === myQueryId) {
              try {
                const delay =
                  backgroundChecks > 0
                    ? this.backgroundStatusCheckDelay
                    : this.backgroundStatusCheckInitDelay;

                if (delay) {
                  await wait(delay);
                  if (signal.aborted || this.currentQueryId !== myQueryId) return;
                }

                const intermediate = await intermediateStateCallback();

                if (signal.aborted || this.currentQueryId !== myQueryId) return;

                this.setState(
                  new QueryState<R, E, I>({
                    loading: true,
                    intermediate,
                    intermediateError,
                    lastData: this.state.getSomeData(),
                  }),
                );

                intermediateError = undefined; // Clear the intermediate error if there was one
              } catch (e) {
                if (signal.aborted || this.currentQueryId !== myQueryId) return;
                if (this.swallowBackgroundError?.(e)) {
                  intermediateError = e;
                } else {
                  return; // Stop the loop on unrecoverable error
                }
              }

              backgroundChecks++;
            }
          })();
        },
      });
    } catch (e) {
      if (this.currentQueryId !== myQueryId) return;
      this.currentRunCancelFn = undefined;
      this.setState(
        new QueryState<R, E>({
          error: QueryManager.remapAxiosCancellationIntoError(e),
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
        signal.throwIfAborted();
        if (this.currentQueryId !== myQueryId) return;

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
          signal.throwIfAborted();
          if (this.currentQueryId !== myQueryId) return;
        }

        data = await this.backgroundStatusCheck(data.state, query, signal);
        intermediateError = undefined; // Clear the intermediate error if there was one
      } catch (e) {
        if (this.currentQueryId !== myQueryId) return;
        if (this.swallowBackgroundError?.(e)) {
          intermediateError = e;
        } else {
          this.currentRunCancelFn = undefined;
          this.setState(
            new QueryState<R, E>({
              error: QueryManager.remapAxiosCancellationIntoError(e),
              lastData: this.state.getSomeData(),
            }),
          );
          return;
        }
      }

      backgroundChecks++;
    }

    if (this.currentQueryId !== myQueryId) return;

    if (data instanceof ResultWithAuxiliaryWork && !data.auxiliaryQueries.length) {
      data = data.result;
    }

    const lastData = this.state.getSomeData();
    if (data instanceof ResultWithAuxiliaryWork) {
      const auxiliaryQueries = data.auxiliaryQueries;
      const numAuxiliaryQueries = auxiliaryQueries.length;
      data = data.result;

      this.setState(
        new QueryState<R, E>({
          data,
          auxiliaryLoading: true,
          lastData,
        }),
      );

      try {
        for (let i = 0; i < numAuxiliaryQueries; i++) {
          signal.throwIfAborted();
          if (this.currentQueryId !== myQueryId) return;

          data = await auxiliaryQueries[i](data, signal);

          if (this.currentQueryId !== myQueryId) return;
          if (i < numAuxiliaryQueries - 1) {
            // Update data in intermediate state
            this.setState(
              new QueryState<R, E>({
                data,
                auxiliaryLoading: true,
                lastData,
              }),
            );
          }
        }
      } catch {}
    }

    if (this.currentQueryId !== myQueryId) return;
    this.currentRunCancelFn = undefined;
    this.setState(
      new QueryState<R, E>({
        data,
        lastData,
      }),
    );
  }

  private trigger() {
    if (this.currentRunCancelFn && !this.state.auxiliaryLoading) {
      // Currently loading main query
      void this.runWhenLoading();
    } else {
      this.setState(
        new QueryState<R, E>({
          loading: true,
          lastData: this.state.getSomeData(),
        }),
      );

      if (this.lastQuery) {
        void this.runWhenIdle();
      } else {
        void this.runWhenInit();
      }
    }
  }

  public runQuery(query: Q): void {
    if (this.terminated) return;
    this.nextQuery = query;
    this.trigger();
  }

  public rerunLastQuery(runInBackground = false): void {
    if (this.terminated) return;
    if (runInBackground && this.currentRunCancelFn) return;
    this.nextQuery = this.lastQuery;
    if (runInBackground) {
      void this.runWhenIdle();
    } else {
      this.trigger();
    }
  }

  public cancelCurrent(message?: string): void {
    if (!this.currentRunCancelFn) return;
    this.currentRunCancelFn(message);
    this.currentRunCancelFn = undefined;
  }

  public getLastQuery(): Q | undefined {
    return this.lastQuery;
  }

  public getLastIntermediateQuery(): unknown {
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
