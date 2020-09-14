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

import axios from 'axios';
import { execSync } from 'child_process';
import path from 'path';
import * as playwright from 'playwright-core';
import { v4 as uuid } from 'uuid';

import { CompactionConfig } from './component/datasources/compaction';
import { Datasource } from './component/datasources/datasource';
import { DatasourcesOverview } from './component/datasources/overview';
import { saveScreenshotIfError } from './util/debug';
import { COORDINATOR_URL } from './util/druid';
import { DRUID_DIR } from './util/druid';
import { UNIFIED_CONSOLE_URL } from './util/druid';
import { createBrowserNormal as createBrowser } from './util/playwright';
import { createPage } from './util/playwright';
import { retryIfJestAssertionError } from './util/retry';

jest.setTimeout(5 * 60 * 1000);

// The workflow in these tests is based on the compaction tutorial:
// https://druid.apache.org/docs/latest/tutorials/tutorial-compaction.html
describe('Auto-compaction', () => {
  let browser: playwright.Browser;
  let page: playwright.Page;

  beforeAll(async () => {
    browser = await createBrowser();
  });

  beforeEach(async () => {
    page = await createPage(browser);
  });

  afterAll(async () => {
    await browser.close();
  });

  it('Compacts segments when max rows per segment is in tuning config', async () => {
    const datasourceName = uuid();
    loadInitialData(datasourceName);

    await saveScreenshotIfError('auto-compaction-', page, async () => {
      const uncompactedNumSegment = 3;
      const numRow = 1412;
      await validateDatasourceStatus(page, datasourceName, uncompactedNumSegment, numRow);

      const compactionConfig = new CompactionConfig({
        skipOffsetFromLatest: 'PT0S',
        tuningConfig: `{
          "type" : "index_parallel",
          "maxRowsInMemory" : 25000,
          "partitionsSpec": {
            "type": "dynamic",
            "maxRowsPerSegment" : 5000000
          }
        }`,
      });
      await configureCompaction(page, datasourceName, compactionConfig);

      // Depending on the number of configured tasks slots, autocompaction may
      // need several iterations if several time chunks need compaction
      let currNumSegment = uncompactedNumSegment;
      await retryIfJestAssertionError(async () => {
        await triggerCompaction();
        currNumSegment = await waitForCompaction(page, datasourceName, currNumSegment);

        const compactedNumSegment = 2;
        expect(currNumSegment).toBe(compactedNumSegment);
      });
    });
  });
});

function loadInitialData(datasourceName: string) {
  const postIndexTask = path.join(DRUID_DIR, 'examples', 'bin', 'post-index-task');
  const ingestionSpec = path.join(
    DRUID_DIR,
    'examples',
    'quickstart',
    'tutorial',
    'compaction-init-index.json',
  );
  const setDatasourceName = `s/compaction-tutorial/${datasourceName}/`;
  const setIntervals = 's|2015-09-12/2015-09-13|2015-09-12/2015-09-12T02:00|'; // shorten to reduce test duration
  execSync(
    `${postIndexTask} \
       --file <(sed -e '${setDatasourceName}' -e '${setIntervals}' ${ingestionSpec}) \
       --url ${COORDINATOR_URL}`,
    {
      shell: 'bash',
      timeout: 3 * 60 * 1000,
    },
  );
}

async function validateDatasourceStatus(
  page: playwright.Page,
  datasourceName: string,
  expectedNumSegment: number,
  expectedNumRow: number,
) {
  await retryIfJestAssertionError(async () => {
    const datasource = await getDatasource(page, datasourceName);
    expect(datasource.availability).toMatch(`Fully available (${expectedNumSegment} segments)`);
    expect(datasource.totalRows).toBe(expectedNumRow);
  });
}

async function getDatasource(page: playwright.Page, datasourceName: string): Promise<Datasource> {
  const datasourcesOverview = new DatasourcesOverview(page, UNIFIED_CONSOLE_URL);
  const datasources = await datasourcesOverview.getDatasources();
  const datasource = datasources.find(t => t.name === datasourceName);
  expect(datasource).toBeDefined();
  return datasource!;
}

async function configureCompaction(
  page: playwright.Page,
  datasourceName: string,
  compactionConfig: CompactionConfig,
) {
  const datasourcesOverview = new DatasourcesOverview(page, UNIFIED_CONSOLE_URL);
  await datasourcesOverview.setCompactionConfiguration(datasourceName, compactionConfig);
}

async function triggerCompaction() {
  const res = await axios.post(`${COORDINATOR_URL}/druid/coordinator/v1/compaction/compact`);
  expect(res.status).toBe(200);
}

async function waitForCompaction(
  page: playwright.Page,
  datasourceName: string,
  prevNumSegment: number,
): Promise<number> {
  await retryIfJestAssertionError(async () => {
    const currNumSegment = await getNumSegment(page, datasourceName);
    expect(currNumSegment).toBeLessThan(prevNumSegment);
  });

  return getNumSegment(page, datasourceName);
}

async function getNumSegment(page: playwright.Page, datasourceName: string): Promise<number> {
  const datasource = await getDatasource(page, datasourceName);
  const currNumSegmentString = datasource!.availability.match(/(\d+)/)![0];
  return Number(currNumSegmentString);
}
