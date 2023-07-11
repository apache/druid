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

import * as JSONBig from 'json-bigint-native';

import { WorkbenchQuery } from '../druid-models';
import { localStorageGetJson, LocalStorageKeys, localStorageSetJson } from '../utils';

export interface WorkbenchQueryHistoryEntry {
  readonly version: string;
  readonly query: WorkbenchQuery;
}

export class WorkbenchHistory {
  static MAX_ENTRIES = 10;

  private static getHistoryVersion(): string {
    return new Date().toISOString().split('.')[0].replace('T', ' ');
  }

  static getHistory(): WorkbenchQueryHistoryEntry[] {
    const possibleQueryHistory = localStorageGetJson(LocalStorageKeys.WORKBENCH_HISTORY);
    return Array.isArray(possibleQueryHistory)
      ? possibleQueryHistory
          .filter(h => h.query)
          .map(h => ({
            ...h,
            query: new WorkbenchQuery(h.query),
          }))
      : [];
  }

  private static setHistory(history: WorkbenchQueryHistoryEntry[]): void {
    localStorageSetJson(LocalStorageKeys.WORKBENCH_HISTORY, history);
  }

  static addQueryToHistory(query: WorkbenchQuery): void {
    const queryHistory = WorkbenchHistory.getHistory();

    // Do not add to history if already the same as the last element in query and context
    if (
      queryHistory.length &&
      JSONBig.stringify(queryHistory[0].query) === JSONBig.stringify(query)
    ) {
      return;
    }

    WorkbenchHistory.setHistory(
      [
        {
          version: WorkbenchHistory.getHistoryVersion(),
          query,
        } as WorkbenchQueryHistoryEntry,
        ...queryHistory,
      ].slice(0, WorkbenchHistory.MAX_ENTRIES),
    );
  }
}
