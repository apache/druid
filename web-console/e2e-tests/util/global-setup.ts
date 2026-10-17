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

import type { FullConfig } from '@playwright/test';
import { expect, request } from '@playwright/test';

/**
 * Waits until the console can run SQL, which is what it needs to function.
 */
export default async function globalSetup(config: FullConfig) {
  const { baseURL } = config.projects[0].use;
  const api = await request.newContext({ baseURL });
  try {
    await expect(async () => {
      const response = await api.post('/druid/v2/sql', { data: { query: 'SELECT 1' } });
      expect(response.status()).toBe(200);
    }).toPass({ timeout: 2 * 60 * 1000, intervals: [1000] });
  } finally {
    await api.dispose();
  }
}
