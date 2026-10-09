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

import type { Datasource } from './component/datasources/overview';
import { DatasourcesOverview } from './component/datasources/overview';
import { readTutorialIngestionSpec, runTask } from './util/druid';
import { expect, test } from './util/fixtures';
import { CLUSTER_STATE_POLL, getDatasourceSegments, getTaskStatuses } from './util/sql';

const NUM_ROWS = 39244;

test.describe('Datasource actions', () => {
  test('Marks the segments unused, used again, then deletes them with a kill task', async ({
    page,
    request,
    newDatasourceName,
  }) => {
    const datasourceName = newDatasourceName('datasource-actions');
    await loadData(request, datasourceName);
    const datasourcesOverview = new DatasourcesOverview(page);

    // Mark unused: the datasource is only shown with "Show unused", as unused
    await datasourcesOverview.runAction(datasourceName, 'Mark as unused all segments');
    await expect
      .poll(() => getDatasourceSegments(request, datasourceName), CLUSTER_STATE_POLL)
      .toEqual({ numSegments: 0, numAvailableSegments: 0, numRows: null });
    await expectInView(datasourcesOverview, datasourceName, false, []);
    await expectInView(datasourcesOverview, datasourceName, true, [
      expect.objectContaining({
        name: datasourceName,
        availability: expect.stringContaining('Unused'),
      }),
    ]);

    // Mark used: the data is back
    await datasourcesOverview.runAction(datasourceName, 'Mark as used all segments', {
      showUnused: true,
    });
    await expect
      .poll(() => getDatasourceSegments(request, datasourceName), CLUSTER_STATE_POLL)
      .toEqual({ numSegments: 1, numAvailableSegments: 1, numRows: NUM_ROWS });
    await expectInView(datasourcesOverview, datasourceName, false, [
      expect.objectContaining({
        name: datasourceName,
        availability: expect.stringContaining('Fully available (1 segment)'),
        totalRows: NUM_ROWS,
      }),
    ]);

    // Mark unused again, and delete the unused segments with a kill task: the datasource is gone
    await datasourcesOverview.runAction(datasourceName, 'Mark as unused all segments');
    await expect
      .poll(() => getDatasourceSegments(request, datasourceName), CLUSTER_STATE_POLL)
      .toEqual({ numSegments: 0, numAvailableSegments: 0, numRows: null });
    await expectInView(datasourcesOverview, datasourceName, true, [
      expect.objectContaining({ name: datasourceName }),
    ]);
    await datasourcesOverview.runAction(datasourceName, 'Delete segments (issue kill task)', {
      showUnused: true,
    });
    await expect
      .poll(() => getTaskStatuses(request, datasourceName), CLUSTER_STATE_POLL)
      .toEqual(['SUCCESS', 'SUCCESS']); // the index task, and the kill task
    await expectInView(datasourcesOverview, datasourceName, true, []);
  });
});

async function loadData(request: APIRequestContext, datasourceName: string) {
  const ingestionSpec = readTutorialIngestionSpec('wikipedia-index.json');
  ingestionSpec.spec.dataSchema.dataSource = datasourceName;
  await runTask(request, ingestionSpec);
  await expect
    .poll(() => getDatasourceSegments(request, datasourceName), CLUSTER_STATE_POLL)
    .toEqual({ numSegments: 1, numAvailableSegments: 1, numRows: NUM_ROWS });
}

async function expectInView(
  datasourcesOverview: DatasourcesOverview,
  datasourceName: string,
  showUnused: boolean,
  expected: (Datasource | ReturnType<typeof expect.objectContaining>)[],
) {
  // Retried in case the view is a step behind the cluster state polled before
  await expect(async () => {
    expect(await datasourcesOverview.getDatasources(datasourceName, { showUnused })).toEqual(
      expected,
    );
  }).toPass({ timeout: 30 * 1000 });
}
