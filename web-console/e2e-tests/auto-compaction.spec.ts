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

import path from 'path';
import type * as playwright from 'playwright-chromium';

import { CompactionConfig } from './component/datasources/compaction';
import type { Datasource } from './component/datasources/datasource';
import { DatasourcesOverview } from './component/datasources/overview';
import { HashedPartitionsSpec } from './component/load-data/config/partition';
import { saveScreenshotIfError } from './util/debug';
import {
  DRUID_EXAMPLES_QUICKSTART_TUTORIAL_DIR,
  runIndexTask,
  UNIFIED_CONSOLE_URL,
} from './util/druid';
import { createBrowser, createPage } from './util/playwright';
import { retryIfJestAssertionError } from './util/retry';
import { waitTillWebConsoleReady } from './util/setup';

jest.setTimeout(5 * 60 * 1000);

// The workflow in these tests is based on the compaction tutorial:
// https://druid.apache.org/docs/latest/tutorials/tutorial-compaction.html
describe('Auto-compaction', () => {
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

  it('Compacts segments from dynamic to hash partitions', async () => {
    const testName = 'autocompaction-dynamic-to-hash';
    const datasourceName = testName + new Date().toISOString();
    loadInitialData(datasourceName);

    await saveScreenshotIfError(testName, page, async () => {
      const uncompactedNumSegment = 3;
      const numRow = 1412;
      await validateDatasourceStatus(page, datasourceName, uncompactedNumSegment, numRow);

      const compactionConfig = new CompactionConfig({
        skipOffsetFromLatest: 'PT0S',
        partitionsSpec: new HashedPartitionsSpec({
          numShards: null,
        }),
      });
      await configureCompaction(page, datasourceName, compactionConfig);

      // Depending on the number of configured tasks slots, autocompaction may
      // need several iterations if several time chunks need compaction
      let currNumSegment = uncompactedNumSegment;
      await retryIfJestAssertionError(async () => {
        await triggerCompaction(page);
        currNumSegment = await waitForCompaction(page, datasourceName, currNumSegment);

        const compactedNumSegment = 2;
        expect(currNumSegment).toBe(compactedNumSegment);
      });
    });
  });
});

function loadInitialData(datasourceName: string) {
  const ingestionSpec = path.join(
    DRUID_EXAMPLES_QUICKSTART_TUTORIAL_DIR,
    'compaction-init-index.json',
  );
  const setDatasourceName = `s/compaction-tutorial/${datasourceName}/`;
  const setIntervals = 's|2015-09-12/2015-09-13|2015-09-12/2015-09-12T02:00|'; // shorten to reduce test duration
  const sedCommands = [setDatasourceName, setIntervals];
  runIndexTask(ingestionSpec, sedCommands);
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

  // Saving the compaction config is not instantaneous
  await retryIfJestAssertionError(async () => {
    const savedCompactionConfig = await datasourcesOverview.getCompactionConfiguration(
      datasourceName,
    );
    expect(savedCompactionConfig).toEqual(compactionConfig);
  });
}

async function triggerCompaction(page: playwright.Page) {
  const datasourcesOverview = new DatasourcesOverview(page, UNIFIED_CONSOLE_URL);
  await datasourcesOverview.triggerCompaction();
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
  const currNumSegmentString = /(\d+)/.exec(datasource.availability)![0];
  return Number(currNumSegmentString);
}
