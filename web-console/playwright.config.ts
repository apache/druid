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

import { defineConfig } from '@playwright/test';

const UNIFIED_CONSOLE_PORT = process.env['DRUID_E2E_TEST_UNIFIED_CONSOLE_PORT'] || '8888';

export default defineConfig({
  testDir: 'e2e-tests',
  // The tests share one Druid cluster (and its task slots)
  workers: 1,
  timeout: 5 * 60 * 1000,
  expect: {
    timeout: 30 * 1000,
    // For polling the cluster state: a task finishing, segments loading
    toPass: { timeout: 2 * 60 * 1000, intervals: [1000] },
  },
  globalSetup: './e2e-tests/util/global-setup.ts',
  reporter: 'list',
  use: {
    browserName: 'chromium',
    baseURL: `http://localhost:${UNIFIED_CONSOLE_PORT}`,
    viewport: { width: 1250, height: 760 },
    actionTimeout: 30 * 1000,
    screenshot: 'only-on-failure',
    trace: 'retain-on-failure',
  },
});
