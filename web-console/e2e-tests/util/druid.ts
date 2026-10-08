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
 * Submits a task (through the console's service, which `request`, the Playwright request fixture, uses) and waits
 * for it to succeed. Its segments are loaded some time after that.
 */
export async function runIndexTask(
  request: APIRequestContext,
  ingestionSpec: IngestionSpec,
): Promise<void> {
  const submitResponse = await request.post('/druid/indexer/v1/task', { data: ingestionSpec });
  expect(submitResponse.ok(), await submitResponse.text()).toBe(true);
  const taskId = ((await submitResponse.json()) as { task: string }).task;

  let statusCode: string | undefined;
  let errorMsg: string | undefined;
  await expect
    .poll(
      async () => {
        const statusResponse = await request.get(
          `/druid/indexer/v1/task/${encodeURIComponent(taskId)}/status`,
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
