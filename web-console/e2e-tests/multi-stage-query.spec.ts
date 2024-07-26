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

import type * as playwright from 'playwright-chromium';

import { WorkbenchOverview } from './component/workbench/overview';
import { saveScreenshotIfError } from './util/debug';
import { DRUID_EXAMPLES_QUICKSTART_TUTORIAL_DIR, UNIFIED_CONSOLE_URL } from './util/druid';
import { createBrowser, createPage } from './util/playwright';
import { waitTillWebConsoleReady } from './util/setup';

jest.setTimeout(5 * 60 * 1000);

describe('Multi-stage query', () => {
  let browser: playwright.Browser;
  let page: playwright.Page;

  beforeAll(async () => {
    await waitTillWebConsoleReady();
    browser = await createBrowser();
  });

  beforeEach(async () => {
    page = await createPage(browser);
  });

  afterAll(async () => {
    await browser.close();
  });

  it('runs a query that reads external data', async () => {
    const workbench = new WorkbenchOverview(page, UNIFIED_CONSOLE_URL);

    await saveScreenshotIfError('multi-stage-query', page, async () => {
      const results = await workbench.runQuery(`WITH ext AS (SELECT *
FROM TABLE(
  EXTERN(
    '{"type":"local","filter":"wikiticker-2015-09-12-sampled.json.gz","baseDir":${JSON.stringify(
      DRUID_EXAMPLES_QUICKSTART_TUTORIAL_DIR,
    )}}',
    '{"type":"json"}'
  )
) EXTEND (channel VARCHAR))
SELECT
  channel,
  CAST(COUNT(*) AS VARCHAR) AS "CountString"
FROM ext
GROUP BY 1
ORDER BY COUNT(*) DESC
LIMIT 10`);
      expect(results).toBeDefined();
      expect(results.length).toBe(10);
      expect(results[0]).toStrictEqual(['#en.wikipedia', '11549']);
      expect(results[1]).toStrictEqual(['#vi.wikipedia', '9747']);
    });
  });
});
