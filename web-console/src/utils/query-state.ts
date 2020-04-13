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

export class QueryState<T> {
  static INIT: QueryState<any> = new QueryState({});

  public state: QueryStateState = 'init';
  public error?: string;
  public data?: T;

  constructor(opts: { loading?: boolean; error?: string; data?: T }) {
    if (opts.error) {
      if (opts.data) {
        throw new Error('can not have both error and data');
      } else {
        this.state = 'error';
        this.error = opts.error;
      }
    } else {
      if (opts.data) {
        this.state = 'data';
        this.data = opts.data;
      } else {
        this.state = opts.loading ? 'loading' : 'init';
      }
    }
  }

  isInit(): boolean {
    return this.state === 'init';
  }

  isLoading(): boolean {
    return this.state === 'loading';
  }
}
