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

import { QueryResult } from 'druid-query-toolkit';

export interface WorkbenchRunningPromise {
  promise: Promise<QueryResult>;
  sqlPrefixLines: number | undefined;
}

export class WorkbenchRunningPromises {
  private static readonly promises: Record<string, WorkbenchRunningPromise> = {};

  static isWorkbenchRunningPromise(x: any): x is WorkbenchRunningPromise {
    return Boolean(x.promise);
  }

  static storePromise(id: string, promise: WorkbenchRunningPromise): void {
    WorkbenchRunningPromises.promises[id] = promise;
  }

  static getPromise(id: string): WorkbenchRunningPromise | undefined {
    return WorkbenchRunningPromises.promises[id];
  }

  static deletePromise(id: string): void {
    delete WorkbenchRunningPromises.promises[id];
  }

  static deletePromises(ids: string[]): void {
    for (const id of ids) {
      delete WorkbenchRunningPromises.promises[id];
    }
  }
}
