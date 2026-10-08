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

import { test as base } from '@playwright/test';

import { deleteDatasource } from './druid';

export { expect } from '@playwright/test';

export const test = base.extend<{
  logFailedResponses: void;
  newDatasourceName: (prefix: string) => string;
}>({
  // Logs every response with an error status (and its body) so that a failure can be understood from the log alone
  logFailedResponses: [
    async ({ page }, use) => {
      page.on('response', async response => {
        if (response.status() < 400) return;

        let bodyText: string;
        try {
          bodyText = await response.text();
        } catch (e) {
          bodyText = `Could not get the body of the error message due to: ${e.message}`;
        }

        console.log(`==============================================`);
        console.log(`Request failed on ${response.url()} (with status ${response.status()})`);
        console.log(`Body: ${bodyText}`);
        console.log(`==============================================`);
      });

      await use();
    },
    { auto: true },
  ],

  // Makes a unique datasource name (the prefix and the time), for the test to create. After the test passes, the
  // datasource is deleted (with its tasks, compaction config and segments). After it fails, it is kept to look into.
  newDatasourceName: [
    async ({ request }, use, testInfo) => {
      const datasourceNames: string[] = [];
      await use(prefix => {
        const datasourceName = prefix + new Date().toISOString();
        datasourceNames.push(datasourceName);
        return datasourceName;
      });

      if (testInfo.status !== testInfo.expectedStatus) {
        for (const datasourceName of datasourceNames) {
          console.log(`Keeping datasource ${datasourceName} of the failed test`);
        }
        return;
      }
      for (const datasourceName of datasourceNames) {
        await deleteDatasource(request, datasourceName);
      }
    },
    { timeout: 2 * 60 * 1000 },
  ],
});
