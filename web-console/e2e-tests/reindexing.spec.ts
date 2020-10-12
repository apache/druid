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
import * as playwright from 'playwright-chromium';

import { DatasourcesOverview } from './component/datasources/overview';
import { IngestionOverview } from './component/ingestion/overview';
import { ConfigureSchemaConfig } from './component/load-data/config/configure-schema';
import { PartitionConfig } from './component/load-data/config/partition';
import { SegmentGranularity } from './component/load-data/config/partition';
import { SingleDimPartitionsSpec } from './component/load-data/config/partition';
import { PublishConfig } from './component/load-data/config/publish';
import { ReindexDataConnector } from './component/load-data/data-connector/reindex';
import { DataLoader } from './component/load-data/data-loader';
import { saveScreenshotIfError } from './util/debug';
import { DRUID_EXAMPLES_QUICKSTART_TUTORIAL_DIR } from './util/druid';
import { UNIFIED_CONSOLE_URL } from './util/druid';
import { runIndexTask } from './util/druid';
import { createBrowser } from './util/playwright';
import { createPage } from './util/playwright';
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

  it('Reindex datasource from dynamic to single dim partitions', async () => {
    const testName = 'reindex-dynamic-to-single-dim-';
    const datasourceName = testName + new Date().toISOString();
    const interval = '2015-09-12/2015-09-13';
    const dataConnector = new ReindexDataConnector(page, {
      datasourceName,
      interval,
    });
    const configureSchemaConfig = new ConfigureSchemaConfig({ rollup: false });
    const partitionConfig = new PartitionConfig({
      segmentGranularity: SegmentGranularity.DAY,
      timeIntervals: interval,
      forceGuaranteedRollup: true,
      partitionsSpec: new SingleDimPartitionsSpec({
        partitionDimension: 'channel',
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
    'Druid row: {' +
      '"__time":1442018818771' +
      ',"isRobot":"false"' +
      ',"countryIsoCode":null' +
      ',"added":"36"' +
      ',"regionName":null' +
      ',"channel":"#en.wikipedia"' +
      ',"delta":"36"' +
      ',"isUnpatrolled":"false"' +
      ',"isNew":"false"' +
      ',"isMinor":"false"' +
      ',"isAnonymous":"false"' +
      ',"deleted":"0"' +
      ',"cityName":null' +
      ',"metroCode":null' +
      ',"namespace":"Talk"' +
      ',"comment":"added project"' +
      ',"countryName":null' +
      ',"page":"Talk:Oswald Tilghman"' +
      ',"user":"GELongstreet"' +
      ',"regionIsoCode":null' +
      '}',
  );
  const lastLine = lines[lines.length - 1];
  expect(lastLine).toBe(
    'Druid row: {' +
      '"__time":1442020314823' +
      ',"isRobot":"false"' +
      ',"countryIsoCode":null' +
      ',"added":"1"' +
      ',"regionName":null' +
      ',"channel":"#en.wikipedia"' +
      ',"delta":"1"' +
      ',"isUnpatrolled":"false"' +
      ',"isNew":"false"' +
      ',"isMinor":"true"' +
      ',"isAnonymous":"false"' +
      ',"deleted":"0"' +
      ',"cityName":null' +
      ',"metroCode":null' +
      ',"namespace":"Main"' +
      ',"comment":"/* History */[[WP:AWB/T|Typo fixing]], [[WP:AWB/T|typo(s) fixed]]: nothern → northern using [[Project:AWB|AWB]]"' +
      ',"countryName":null' +
      ',"page":"Hapoel Katamon Jerusalem F.C."' +
      ',"user":"The Quixotic Potato"' +
      ',"regionIsoCode":null' +
      '}',
  );
}

async function validateTaskStatus(page: playwright.Page, datasourceName: string) {
  const ingestionOverview = new IngestionOverview(page, UNIFIED_CONSOLE_URL);

  await retryIfJestAssertionError(async () => {
    const tasks = await ingestionOverview.getTasks();
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
