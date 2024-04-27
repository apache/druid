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

import { DatasourcesOverview } from './component/datasources/overview';
import { TasksOverview } from './component/ingestion/overview';
import { ConfigureSchemaConfig } from './component/load-data/config/configure-schema';
import {
  PartitionConfig,
  RangePartitionsSpec,
  SegmentGranularity,
} from './component/load-data/config/partition';
import { PublishConfig } from './component/load-data/config/publish';
import { ReindexDataConnector } from './component/load-data/data-connector/reindex';
import { DataLoader } from './component/load-data/data-loader';
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

describe('Reindexing from Druid', () => {
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

  it('Reindex datasource from dynamic to range partitions', async () => {
    const testName = 'reindex-dynamic-to-range';
    const datasourceName = testName + new Date().toISOString();
    const interval = '2015-09-12/2015-09-13';
    const dataConnector = new ReindexDataConnector(page, {
      datasourceName,
      interval,
    });
    const configureSchemaConfig = new ConfigureSchemaConfig({ rollup: false });
    const partitionConfig = new PartitionConfig({
      segmentGranularity: SegmentGranularity.DAY,
      timeIntervals: null,
      partitionsSpec: new RangePartitionsSpec({
        partitionDimensions: ['channel'],
        targetRowsPerSegment: 10_000,
        maxRowsPerSegment: null,
      }),
    });
    const publishConfig = new PublishConfig({ datasourceName: datasourceName });

    const dataLoader = new DataLoader({
      page: page,
      unifiedConsoleUrl: UNIFIED_CONSOLE_URL,
      connector: dataConnector,
      connectValidator: validateConnectLocalData,
      configureSchemaConfig: configureSchemaConfig,
      partitionConfig: partitionConfig,
      publishConfig: publishConfig,
    });

    loadInitialData(datasourceName);

    await saveScreenshotIfError(testName, page, async () => {
      const numInitialSegment = 1;
      await validateDatasourceStatus(page, datasourceName, numInitialSegment);

      await dataLoader.load();
      await validateTaskStatus(page, datasourceName);

      const numReindexedSegment = 4; // 39k rows into segments of ~10k rows
      await validateDatasourceStatus(page, datasourceName, numReindexedSegment);
    });
  });
});

function loadInitialData(datasourceName: string) {
  const ingestionSpec = path.join(DRUID_EXAMPLES_QUICKSTART_TUTORIAL_DIR, 'wikipedia-index.json');
  const setDatasourceName = `s/wikipedia/${datasourceName}/`;
  const sedCommands = [setDatasourceName];
  runIndexTask(ingestionSpec, sedCommands);
}

function validateConnectLocalData(preview: string) {
  const lines = preview.split('\n');
  expect(lines.length).toBe(500);
  const firstLine = lines[0];
  expect(firstLine).toBe(
    '[Druid row: {' +
      '"__time":1442018818771' +
      ',"channel":"#en.wikipedia"' +
      ',"comment":"added project"' +
      ',"isAnonymous":"false"' +
      ',"isMinor":"false"' +
      ',"isNew":"false"' +
      ',"isRobot":"false"' +
      ',"isUnpatrolled":"false"' +
      ',"namespace":"Talk"' +
      ',"page":"Talk:Oswald Tilghman"' +
      ',"user":"GELongstreet"' +
      ',"added":36' +
      ',"deleted":0' +
      ',"delta":36' +
      '}]',
  );
  const lastLine = lines[lines.length - 1];
  expect(lastLine).toBe(
    '[Druid row: {' +
      '"__time":1442020314823' +
      ',"channel":"#en.wikipedia"' +
      ',"comment":"/* History */[[WP:AWB/T|Typo fixing]], [[WP:AWB/T|typo(s) fixed]]: nothern → northern using [[Project:AWB|AWB]]"' +
      ',"isAnonymous":"false"' +
      ',"isMinor":"true"' +
      ',"isNew":"false"' +
      ',"isRobot":"false"' +
      ',"isUnpatrolled":"false"' +
      ',"namespace":"Main"' +
      ',"page":"Hapoel Katamon Jerusalem F.C."' +
      ',"user":"The Quixotic Potato"' +
      ',"added":1' +
      ',"deleted":0' +
      ',"delta":1' +
      '}]',
  );
}

async function validateTaskStatus(page: playwright.Page, datasourceName: string) {
  const tasksOverview = new TasksOverview(page, UNIFIED_CONSOLE_URL);

  await retryIfJestAssertionError(async () => {
    const tasks = await tasksOverview.getTasks();
    const task = tasks.find(t => t.datasource === datasourceName);
    expect(task).toBeDefined();
    expect(task!.status).toMatch('SUCCESS');
  });
}

async function validateDatasourceStatus(
  page: playwright.Page,
  datasourceName: string,
  expectedNumSegment: number,
) {
  const datasourcesOverview = new DatasourcesOverview(page, UNIFIED_CONSOLE_URL);
  const numSegmentString = `${expectedNumSegment} segment` + (expectedNumSegment !== 1 ? 's' : '');

  await retryIfJestAssertionError(async () => {
    const datasources = await datasourcesOverview.getDatasources();
    const datasource = datasources.find(t => t.name === datasourceName);
    expect(datasource).toBeDefined();
    expect(datasource!.availability).toMatch(`Fully available (${numSegmentString})`);
    expect(datasource!.totalRows).toBe(39244);
  });
}
