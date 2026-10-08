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

import type { APIRequestContext } from '@playwright/test';

/**
 * For expect.poll() on the cluster state: a task finishing, segments loading.
 */
export const CLUSTER_STATE_POLL = { timeout: 2 * 60 * 1000, intervals: [1000] };

/**
 * Runs a SQL query through the console's service (`request` is the Playwright request fixture, which uses the
 * console's URL), with `parameters` for its `?` placeholders.
 */
export async function querySql(
  request: APIRequestContext,
  query: string,
  parameters: string[] = [],
): Promise<Record<string, unknown>[]> {
  const response = await request.post('/druid/v2/sql', {
    data: { query, parameters: parameters.map(value => ({ type: 'VARCHAR', value })) },
  });
  if (!response.ok()) {
    throw new Error(`SQL query failed with status ${response.status()}: ${await response.text()}`);
  }
  return (await response.json()) as Record<string, unknown>[];
}

/**
 * The statuses of the tasks of a datasource, oldest first. Only the tasks that were submitted, not the subtasks they
 * started (which are in the group of the task that started them).
 */
export async function getTaskStatuses(
  request: APIRequestContext,
  datasource: string,
): Promise<string[]> {
  const rows = await querySql(
    request,
    `SELECT "status"
FROM sys.tasks
WHERE "datasource" = ? AND "task_id" = "group_id"
ORDER BY "created_time"`,
    [datasource],
  );
  return rows.map(row => String(row['status']));
}

export interface DatasourceSegments {
  readonly numSegments: number;
  readonly numAvailableSegments: number;
  readonly numRows: number | null;
}

/**
 * The used segments (published and not overshadowed) of a datasource, how many of them are available (loaded) and
 * the rows in them.
 */
export async function getDatasourceSegments(
  request: APIRequestContext,
  datasource: string,
): Promise<DatasourceSegments> {
  const [row] = await querySql(
    request,
    `SELECT
  COUNT(*) AS "numSegments",
  COALESCE(SUM("is_available"), 0) AS "numAvailableSegments",
  SUM("num_rows") AS "numRows"
FROM sys.segments
WHERE "datasource" = ? AND "is_published" = 1 AND "is_overshadowed" = 0`,
    [datasource],
  );
  // Druid returns no row (rather than a row of zeros) when there are no segments
  if (!row) return { numSegments: 0, numAvailableSegments: 0, numRows: null };
  return {
    numSegments: Number(row['numSegments']),
    numAvailableSegments: Number(row['numAvailableSegments']),
    numRows: row['numRows'] == null ? null : Number(row['numRows']),
  };
}
