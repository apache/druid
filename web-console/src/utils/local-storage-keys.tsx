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

function noLocalStorage(): boolean {
  try {
    return typeof localStorage === 'undefined';
  } catch {
    return true;
  }
}

export const LocalStorageKeys = {
  CAPABILITIES_OVERRIDE: 'capabilities-override' as const,
  INGESTION_SPEC: 'ingestion-spec' as const,
  BATCH_INGESTION_SPEC: 'batch-ingestion-spec' as const,
  STREAMING_INGESTION_SPEC: 'streaming-ingestion-spec' as const,
  DATASOURCE_TABLE_COLUMN_SELECTION: 'datasource-table-column-selection' as const,
  SEGMENT_TABLE_COLUMN_SELECTION: 'segment-table-column-selection' as const,
  SUPERVISOR_TABLE_COLUMN_SELECTION: 'supervisor-table-column-selection' as const,
  TASK_TABLE_COLUMN_SELECTION: 'task-table-column-selection' as const,
  SERVICE_TABLE_COLUMN_SELECTION: 'service-table-column-selection' as const,
  LOOKUP_TABLE_COLUMN_SELECTION: 'lookup-table-column-selection' as const,
  QUERY_KEY: 'druid-console-query' as const,
  QUERY_CONTEXT: 'query-context' as const,
  INGESTION_VIEW_PANE_SIZE: 'ingestion-view-pane-size' as const,
  TASKS_REFRESH_RATE: 'task-refresh-rate' as const,
  DATASOURCES_REFRESH_RATE: 'datasources-refresh-rate' as const,
  SEGMENTS_REFRESH_RATE: 'segments-refresh-rate' as const,
  SERVICES_REFRESH_RATE: 'services-refresh-rate' as const,
  SUPERVISORS_REFRESH_RATE: 'supervisors-refresh-rate' as const,
  LOOKUPS_REFRESH_RATE: 'lookups-refresh-rate' as const,
  QUERY_HISTORY: 'query-history' as const,
  LIVE_QUERY_MODE: 'live-query-mode' as const,

  WORKBENCH_QUERIES: 'workbench-queries' as const,
  WORKBENCH_LAST_TAB: 'workbench-last-tab' as const,
  WORKBENCH_PANE_SIZE: 'workbench-pane-size' as const,
  WORKBENCH_HISTORY: 'workbench-history' as const,
  WORKBENCH_TASK_PANEL: 'workbench-task-panel' as const,

  SQL_DATA_LOADER_CONTENT: 'sql-data-loader-content' as const,
};
export type LocalStorageKeys = typeof LocalStorageKeys[keyof typeof LocalStorageKeys];

// ----------------------------

let localStorageNamespace: string | undefined;

export function setLocalStorageNamespace(namespace: string) {
  localStorageNamespace = namespace;
}

function prependNamespace(key: string): string {
  return localStorageNamespace ? `${localStorageNamespace}:${key}` : key;
}

export function localStorageSet(key: LocalStorageKeys, value: string): void {
  if (noLocalStorage()) return;
  try {
    localStorage.setItem(prependNamespace(key), value);
  } catch (e) {
    console.error('Issue setting local storage key', e);
  }
}

export function localStorageSetJson(key: LocalStorageKeys, value: any): void {
  localStorageSet(key, JSONBig.stringify(value));
}

export function localStorageGet(key: LocalStorageKeys): string | undefined {
  if (noLocalStorage()) return;
  try {
    return localStorage.getItem(prependNamespace(key)) || localStorage.getItem(key) || undefined;
  } catch (e) {
    console.error('Issue getting local storage key', e);
    return;
  }
}

export function localStorageGetJson(key: LocalStorageKeys): any {
  const value = localStorageGet(key);
  if (!value) return;
  try {
    return JSONBig.parse(value);
  } catch {
    return;
  }
}

export function localStorageRemove(key: LocalStorageKeys): void {
  if (noLocalStorage()) return;
  try {
    localStorage.removeItem(prependNamespace(key));
  } catch (e) {
    console.error('Issue removing local storage key', e);
  }
}
