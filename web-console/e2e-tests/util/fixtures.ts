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

export { expect } from '@playwright/test';

export const test = base.extend<{ logFailedResponses: void }>({
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
});
