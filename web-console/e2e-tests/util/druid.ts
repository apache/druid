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

import type { APIRequestContext, APIResponse } from '@playwright/test';
import { expect } from '@playwright/test';
import { readFileSync } from 'fs';
import path from 'path';

import type { IngestionSpec } from '../../src/druid-models';

const UTIL_DIR = __dirname;
const E2E_TEST_DIR = path.dirname(UTIL_DIR);
const WEB_CONSOLE_DIR = path.dirname(E2E_TEST_DIR);
const DRUID_DIR = path.dirname(WEB_CONSOLE_DIR);
export const DRUID_EXAMPLES_QUICKSTART_TUTORIAL_DIR = path.join(
  DRUID_DIR,
  'examples',
  'quickstart',
  'tutorial',
);

/**
 * Reads one of the ingestion specs of the tutorials (in examples/quickstart/tutorial), reading its input from this
 * checkout (rather than from `quickstart/tutorial/` relative to where Druid runs).
 */
export function readTutorialIngestionSpec(fileName: string): IngestionSpec {
  const ingestionSpec = JSON.parse(
    readFileSync(path.join(DRUID_EXAMPLES_QUICKSTART_TUTORIAL_DIR, fileName), 'utf-8'),
  ) as IngestionSpec;
  ingestionSpec.spec.ioConfig.inputSource!.baseDir = DRUID_EXAMPLES_QUICKSTART_TUTORIAL_DIR;
  return ingestionSpec;
}

/**
 * Encodes a datasource name (or other value) for a URL path, like Api.encodePath in src/singletons/api.ts (which
 * can't be imported here).
 */
function encodePath(path: string): string {
  return path.replace(/[#%&';?[\\\]^|]/g, c => '%' + c.charCodeAt(0).toString(16).toUpperCase());
}

/**
 * Submits a task (through the console's service, which `request`, the Playwright request fixture, uses) and waits
 * for it to succeed. For an ingestion task, its segments are loaded some time after that.
 */
export async function runTask(
  request: APIRequestContext,
  task: IngestionSpec | Record<string, unknown>,
): Promise<void> {
  const submitResponse = await request.post('/druid/indexer/v1/task', { data: task });
  expect(submitResponse.ok(), await submitResponse.text()).toBe(true);
  const taskId = ((await submitResponse.json()) as { task: string }).task;

  let statusCode: string | undefined;
  let errorMsg: string | undefined;
  await expect
    .poll(
      async () => {
        const statusResponse = await request.get(
          `/druid/indexer/v1/task/${encodePath(taskId)}/status`,
        );
        expect(statusResponse.ok(), await statusResponse.text()).toBe(true);
        const { status } = (await statusResponse.json()) as {
          status: { statusCode: string; errorMsg?: string };
        };
        ({ statusCode, errorMsg } = status);
        return statusCode;
      },
      { timeout: 3 * 60 * 1000, intervals: [1000], message: `task ${taskId} to finish` },
    )
    .not.toBe('RUNNING');

  // Fail here, with why the task failed, rather than time out waiting for its segments
  expect(statusCode, errorMsg).toBe('SUCCESS');
}

/**
 * Removes a datasource that a test created, and everything about it: stops its tasks, removes its compaction config
 * and permanently deletes its segments.
 */
export async function deleteDatasource(
  request: APIRequestContext,
  datasource: string,
): Promise<void> {
  const encodedDatasource = encodePath(datasource);
  const expectOk = async (response: APIResponse, allowNotFound = false) => {
    if (allowNotFound && response.status() === 404) return;
    expect(response.ok(), `${response.url()}: ${await response.text()}`).toBe(true);
  };

  await expectOk(
    await request.post(`/druid/indexer/v1/datasources/${encodedDatasource}/shutdownAllTasks`),
    true, // It has no running tasks
  );
  await expectOk(
    await request.delete(`/druid/indexer/v1/compaction/config/datasources/${encodedDatasource}`),
    true, // It has no compaction config
  );
  await expectOk(
    await request.delete(`/druid/indexer/v1/datasources/${encodedDatasource}`),
    true, // It has no segments
  );
  await runTask(request, {
    type: 'kill',
    dataSource: datasource,
    interval: '1000-01-01/3000-01-01',
  });
}

/**
 * The retention rules of a datasource (none when it uses the cluster default rules).
 */
export async function getRetentionRules(
  request: APIRequestContext,
  datasourceName: string,
): Promise<Record<string, unknown>[]> {
  const response = await request.get(`/druid/coordinator/v1/rules/${encodePath(datasourceName)}`);
  expect(response.status(), await response.text()).toBe(200);
  return (await response.json()) as Record<string, unknown>[];
}
