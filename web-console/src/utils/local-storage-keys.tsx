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

export const LocalStorageKeys = {
  DATASOURCE_TABLE_COLUMN_SELECTION: "datasource-table-column-selection" as "datasource-table-column-selection",
  SEGMENT_TABLE_COLUMN_SELECTION: "segment-table-column-selection" as "segment-table-column-selection",
  SUPERVISOR_TABLE_COLUMN_SELECTION: "supervisor-table-column-selection" as "supervisor-table-column-selection",
  TASK_TABLE_COLUMN_SELECTION: "task-table-column-selection" as "task-table-column-selection",
  SERVER_TABLE_COLUMN_SELECTION: "historical-table-column-selection" as "historical-table-column-selection",
  MIDDLEMANAGER_TABLE_COLUMN_SELECTION: "middleManager-table-column-selection" as "middleManager-table-column-selection",
  LOOKUP_TABLE_COLUMN_SELECTION: "lookup-table-column-selection" as "lookup-table-column-selection",
  QUERY_KEY: 'druid-console-query' as 'druid-console-query'

};
export type LocalStorageKeys = typeof LocalStorageKeys[keyof typeof LocalStorageKeys];

// ----------------------------

export function localStorageSet(key: LocalStorageKeys, value: string): void {
  if (typeof localStorage === 'undefined') return;
  localStorage.setItem(key, value);
}

export function localStorageGet(key: LocalStorageKeys): string | null {
  if (typeof localStorage === 'undefined') return null;
  return localStorage.getItem(key);
}
