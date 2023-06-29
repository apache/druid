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

export type QueryStateState = 'init' | 'loading' | 'data' | 'error';

export interface QueryStateOptions<T, E extends Error = Error, I = never> {
  loading?: boolean;
  intermediate?: I;
  intermediateError?: Error;
  error?: E;
  data?: T;
  lastData?: T;
}

export class QueryState<T, E extends Error = Error, I = never> {
  static INIT: QueryState<any, any> = new QueryState({});
  static LOADING: QueryState<any> = new QueryState({ loading: true });

  public state: QueryStateState = 'init';
  public intermediate?: I;
  public intermediateError?: Error;
  public error?: E;
  public data?: T;
  public lastData?: T;

  constructor(opts: QueryStateOptions<T, E, I>) {
    const hasData = typeof opts.data !== 'undefined';
    if (typeof opts.error !== 'undefined') {
      if (hasData) {
        throw new Error('can not have both error and data');
      } else {
        this.state = 'error';
        this.error = opts.error;
      }
    } else {
      if (hasData) {
        this.state = 'data';
        this.data = opts.data;
      } else if (opts.loading) {
        this.state = 'loading';
        this.intermediate = opts.intermediate;
        this.intermediateError = opts.intermediateError;
      } else {
        this.state = 'init';
      }
    }
    this.lastData = opts.lastData;
  }

  isInit(): boolean {
    return this.state === 'init';
  }

  isLoading(): boolean {
    return this.state === 'loading';
  }

  get loading(): boolean {
    return this.state === 'loading';
  }

  isError(): boolean {
    return this.state === 'error';
  }

  getErrorMessage(): string | undefined {
    const { error } = this;
    if (!error) return;
    return error.message;
  }

  isEmpty(): boolean {
    const { data } = this;
    return Boolean(data && Array.isArray(data) && data.length === 0);
  }

  getSomeData(): T | undefined {
    return this.data || this.lastData;
  }
}
