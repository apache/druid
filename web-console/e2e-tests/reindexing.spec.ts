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

import type { APIRequestContext } from '@playwright/test';

import { loadData } from './component/load-data/data-loader';
import { readTutorialIngestionSpec, runTask } from './util/druid';
import { expect, test } from './util/fixtures';
import { CLUSTER_STATE_POLL, getDatasourceSegments, getTaskStatuses } from './util/sql';

test.describe('Reindexing from Druid', () => {
  test('Reindex datasource from dynamic to range partitions', async ({
    page,
    request,
    newDatasourceName,
  }) => {
    const datasourceName = newDatasourceName('reindex-dynamic-to-range');
    await loadInitialData(request, datasourceName);

    await expect
      .poll(() => getDatasourceSegments(request, datasourceName), CLUSTER_STATE_POLL)
      .toEqual({ numSegments: 1, numAvailableSegments: 1, numRows: 39244 });

    await loadData(page, {
      connector: { type: 'reindex', datasourceName, interval: '2015-09-12/2015-09-13' },
      validateConnect: validateConnectLocalData,
      rollup: false,
      segmentGranularity: 'day',
      partitionsSpec: {
        type: 'range',
        partitionDimensions: ['channel'],
        targetRowsPerSegment: 10_000,
        maxRowsPerSegment: null,
      },
      datasourceName,
    });
    // The initial load and the reindexing
    await expect
      .poll(() => getTaskStatuses(request, datasourceName), CLUSTER_STATE_POLL)
      .toEqual(['SUCCESS', 'SUCCESS']);

    // 39k rows into segments of ~10k rows
    await expect
      .poll(() => getDatasourceSegments(request, datasourceName), CLUSTER_STATE_POLL)
      .toEqual({ numSegments: 4, numAvailableSegments: 4, numRows: 39244 });
  });
});

async function loadInitialData(request: APIRequestContext, datasourceName: string) {
  const ingestionSpec = readTutorialIngestionSpec('wikipedia-index.json');
  ingestionSpec.spec.dataSchema.dataSource = datasourceName;
  await runTask(request, ingestionSpec);
}

function validateConnectLocalData(lines: string[]) {
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
