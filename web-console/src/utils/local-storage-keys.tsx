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

export const LocalStorageKeys = {
  CAPABILITIES_OVERRIDE: 'capabilities-override' as 'capabilities-override',
  INGESTION_SPEC: 'ingestion-spec' as 'ingestion-spec',
  DATASOURCE_TABLE_COLUMN_SELECTION: 'datasource-table-column-selection' as 'datasource-table-column-selection',
  SEGMENT_TABLE_COLUMN_SELECTION: 'segment-table-column-selection' as 'segment-table-column-selection',
  SUPERVISOR_TABLE_COLUMN_SELECTION: 'supervisor-table-column-selection' as 'supervisor-table-column-selection',
  TASK_TABLE_COLUMN_SELECTION: 'task-table-column-selection' as 'task-table-column-selection',
  SERVICE_TABLE_COLUMN_SELECTION: 'service-table-column-selection' as 'service-table-column-selection',
  LOOKUP_TABLE_COLUMN_SELECTION: 'lookup-table-column-selection' as 'lookup-table-column-selection',
  QUERY_KEY: 'druid-console-query' as 'druid-console-query',
  QUERY_CONTEXT: 'query-context' as 'query-context',
  INGESTION_VIEW_PANE_SIZE: 'ingestion-view-pane-size' as 'ingestion-view-pane-size',
  QUERY_VIEW_PANE_SIZE: 'query-view-pane-size' as 'query-view-pane-size',
  TASKS_REFRESH_RATE: 'task-refresh-rate' as 'task-refresh-rate',
  DATASOURCES_REFRESH_RATE: 'datasources-refresh-rate' as 'datasources-refresh-rate',
  SEGMENTS_REFRESH_RATE: 'segments-refresh-rate' as 'segments-refresh-rate',
  SERVICES_REFRESH_RATE: 'services-refresh-rate' as 'services-refresh-rate',
  SUPERVISORS_REFRESH_RATE: 'supervisors-refresh-rate' as 'supervisors-refresh-rate',
  LOOKUPS_REFRESH_RATE: 'lookups-refresh-rate' as 'lookups-refresh-rate',
  QUERY_HISTORY: 'query-history' as 'query-history',
  LIVE_QUERY_MODE: 'live-query-mode' as 'live-query-mode',
};
export type LocalStorageKeys = typeof LocalStorageKeys[keyof typeof LocalStorageKeys];

// ----------------------------

export function localStorageSet(key: LocalStorageKeys, value: string): void {
  if (typeof localStorage === 'undefined') return;
  localStorage.setItem(key, value);
}

export function localStorageSetJson(key: LocalStorageKeys, value: any): void {
  localStorageSet(key, JSONBig.stringify(value));
}

export function localStorageGet(key: LocalStorageKeys): string | undefined {
  if (typeof localStorage === 'undefined') return;
  return localStorage.getItem(key) || undefined;
}

export function localStorageGetJson(key: LocalStorageKeys): any {
  const value = localStorageGet(key);
  if (!value) return;
  try {
    return JSON.parse(value);
  } catch {
    return;
  }
}
