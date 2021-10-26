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

import * as playwright from 'playwright-chromium';

import { IngestionOverview } from './component/ingestion/overview';
import { ConfigureSchemaConfig } from './component/load-data/config/configure-schema';
import { ConfigureTimestampConfig } from './component/load-data/config/configure-timestamp';
import { PartitionConfig, SegmentGranularity } from './component/load-data/config/partition';
import { PublishConfig } from './component/load-data/config/publish';
import { LocalFileDataConnector } from './component/load-data/data-connector/local-file';
import { DataLoader } from './component/load-data/data-loader';
import { saveScreenshotIfError } from './util/debug';
import { DRUID_EXAMPLES_QUICKSTART_TUTORIAL_DIR, UNIFIED_CONSOLE_URL } from './util/druid';
import { createBrowser, createPage } from './util/playwright';
import { retryIfJestAssertionError } from './util/retry';
import { waitTillWebConsoleReady } from './util/setup';

jest.setTimeout(5 * 60 * 1000);

describe('Tutorial: Loading a file', () => {
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

  it('Loads data from local disk', async () => {
    const testName = 'load-data-from-local-disk-';
    const datasourceName = testName + new Date().toISOString();
    const dataLoader = new DataLoader({
      page: page,
      unifiedConsoleUrl: UNIFIED_CONSOLE_URL,
      connector: new LocalFileDataConnector(page, {
        baseDirectory: DRUID_EXAMPLES_QUICKSTART_TUTORIAL_DIR,
        fileFilter: 'wikiticker-2015-09-12-sampled.json.gz',
      }),
      connectValidator: validateConnectLocalData,
      configureTimestampConfig: new ConfigureTimestampConfig({
        timestampExpression: 'timestamp_parse("time") + 1',
      }),
      configureSchemaConfig: new ConfigureSchemaConfig({ rollup: false }),
      partitionConfig: new PartitionConfig({
        segmentGranularity: SegmentGranularity.DAY,
        timeIntervals: null,
        partitionsSpec: null,
      }),
      publishConfig: new PublishConfig({ datasourceName: datasourceName }),
    });

    await saveScreenshotIfError(testName, page, async () => {
      await dataLoader.load();
      await validateShowAllTasksAction(page);
    });
  });
});

function validateConnectLocalData(preview: string) {
  const lines = preview.split('\n');
  expect(lines.length).toBe(500);
  const firstLine = lines[0];
  expect(firstLine).toBe(
    '{' +
      '"time":"2015-09-12T00:46:58.771Z"' +
      ',"channel":"#en.wikipedia"' +
      ',"cityName":null' +
      ',"comment":"added project"' +
      ',"countryIsoCode":null' +
      ',"countryName":null' +
      ',"isAnonymous":false' +
      ',"isMinor":false' +
      ',"isNew":false' +
      ',"isRobot":false' +
      ',"isUnpatrolled":false' +
      ',"metroCode":null' +
      ',"namespace":"Talk"' +
      ',"page":"Talk:Oswald Tilghman"' +
      ',"regionIsoCode":null' +
      ',"regionName":null' +
      ',"user":"GELongstreet"' +
      ',"delta":36' +
      ',"added":36' +
      ',"deleted":0' +
      '}',
  );
  const lastLine = lines[lines.length - 1];
  expect(lastLine).toBe(
    '{' +
      '"time":"2015-09-12T01:11:54.823Z"' +
      ',"channel":"#en.wikipedia"' +
      ',"cityName":null' +
      ',"comment":"/* History */[[WP:AWB/T|Typo fixing]], [[WP:AWB/T|typo(s) fixed]]: nothern → northern using [[Project:AWB|AWB]]"' +
      ',"countryIsoCode":null' +
      ',"countryName":null' +
      ',"isAnonymous":false' +
      ',"isMinor":true' +
      ',"isNew":false' +
      ',"isRobot":false' +
      ',"isUnpatrolled":false' +
      ',"metroCode":null' +
      ',"namespace":"Main"' +
      ',"page":"Hapoel Katamon Jerusalem F.C."' +
      ',"regionIsoCode":null' +
      ',"regionName":null' +
      ',"user":"The Quixotic Potato"' +
      ',"delta":1' +
      ',"added":1' +
      ',"deleted":0' +
      '}',
  );
}

async function validateShowAllTasksAction(page: playwright.Page) {
  const ingestionOverview = new IngestionOverview(page, UNIFIED_CONSOLE_URL);

  await retryIfJestAssertionError(async () => {
    const targetTask = await ingestionOverview.showAllTasks();
    expect(targetTask).toEqual([['', '', '', '', '', '', '', undefined, undefined]]);
  });
}
