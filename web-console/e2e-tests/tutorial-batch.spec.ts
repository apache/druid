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

import type { Page } from '@playwright/test';
import { T } from 'druid-query-toolkit';

import { DatasourcesOverview } from './component/datasources/overview';
import { TasksOverview } from './component/ingestion/overview';
import { loadData } from './component/load-data/data-loader';
import { WorkbenchOverview } from './component/workbench/overview';
import { DRUID_EXAMPLES_QUICKSTART_TUTORIAL_DIR } from './util/druid';
import { expect, test } from './util/fixtures';
import { CLUSTER_STATE_POLL, getDatasourceSegments, getTaskStatuses } from './util/sql';

const ALL_SORTS_OF_CHARS = '<>|!@#$%^&`\'".,:;\\*()[]{}Україна 한국 中国!?~';

test.describe('Tutorial: Loading a file', () => {
  test('Loads data from local disk', async ({ page, request, newDatasourceName }) => {
    const datasourceName = newDatasourceName('load-data-from-local-disk' + ALL_SORTS_OF_CHARS);
    await loadData(page, {
      connector: {
        type: 'local',
        baseDirectory: DRUID_EXAMPLES_QUICKSTART_TUTORIAL_DIR,
        fileFilter: 'wikiticker-2015-09-12-sampled.json.gz',
      },
      validateConnect: validateConnectLocalData,
      timestampExpression: 'timestamp_parse("time") + 1',
      rollup: false,
      segmentGranularity: 'day',
      datasourceName,
    });

    await expect
      .poll(() => getTaskStatuses(request, datasourceName), CLUSTER_STATE_POLL)
      .toEqual(['SUCCESS']);
    await expect
      .poll(() => getDatasourceSegments(request, datasourceName), CLUSTER_STATE_POLL)
      .toEqual({ numSegments: 1, numAvailableSegments: 1, numRows: 39244 });

    // Now that the cluster has the data, the views should show it
    await validateTasksView(page, datasourceName);
    await validateDatasourcesView(page, datasourceName);
    await validateQuery(page, datasourceName);
  });
});

function validateConnectLocalData(lines: string[]) {
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

async function validateTasksView(page: Page, datasourceName: string) {
  const tasksOverview = new TasksOverview(page);

  // Retried in case the view is a step behind the cluster state polled above
  await expect(async () => {
    const tasks = await tasksOverview.getTasks(datasourceName);
    const task = tasks.find(t => t.datasource === datasourceName);
    expect(task).toBeDefined();
    expect(task!.status).toMatch('SUCCESS');
  }).toPass({ timeout: 30 * 1000 });
}

async function validateDatasourcesView(page: Page, datasourceName: string) {
  const datasourcesOverview = new DatasourcesOverview(page);

  // Retried in case the view is a step behind the cluster state polled above
  await expect(async () => {
    const datasources = await datasourcesOverview.getDatasources(datasourceName);
    const datasource = datasources.find(t => t.name === datasourceName);
    expect(datasource).toBeDefined();
    expect(datasource!.availability).toMatch('Fully available (1 segment)');
    expect(datasource!.totalRows).toBe(39244);
  }).toPass({ timeout: 30 * 1000 });
}

async function validateQuery(page: Page, datasourceName: string) {
  const workbench = new WorkbenchOverview(page);
  const query = `SELECT * FROM ${T(datasourceName)} ORDER BY __time`;
  let results!: string[][];
  // The datasource can be available before the Broker's SQL schema knows about it
  await expect(async () => {
    results = await workbench.runQuery(query);
    expect(results.length).toBeGreaterThan(0);
  }).toPass();
  expect(results[0]).toStrictEqual([
    /* __time */ '2015-09-12T00:46:58.772Z',
    /* time */ '2015-09-12T00:46:58.771Z',
    /* channel */ '#en.wikipedia',
    /* cityName */ 'null',
    /* comment */ 'added project',
    /* countryIsoCode */ 'null',
    /* countryName */ 'null',
    /* isAnonymous */ 'false',
    /* isMinor */ 'false',
    /* isNew */ 'false',
    /* isRobot */ 'false',
    /* isUnpatrolled */ 'false',
    /* metroCode */ 'null',
    /* namespace */ 'Talk',
    /* page */ 'Talk:Oswald Tilghman',
    /* regionIsoCode */ 'null',
    /* regionName */ 'null',
    /* user */ 'GELongstreet',
    /* added */ '36',
    /* delta */ '36',
    /* deleted */ '0',
  ]);
}
