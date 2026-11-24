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

import { DatasourcesOverview } from './component/datasources/overview';
import { getRetentionRules, readTutorialIngestionSpec, runTask } from './util/druid';
import { expect, test } from './util/fixtures';
import { CLUSTER_STATE_POLL, getDatasourceSegments } from './util/sql';

test.describe('Retention rules', () => {
  test('Sets a loadForever rule, then replaces it with a dropForever rule, which drops the data', async ({
    page,
    request,
    newDatasourceName,
  }) => {
    const datasourceName = newDatasourceName('retention-rules');
    await loadData(request, datasourceName);
    const datasourcesOverview = new DatasourcesOverview(page);

    // loadForever (as "New rule" makes it, with 2 replicas): the data stays loaded
    await datasourcesOverview.setRetentionRules(datasourceName, ['loadForever'], 'Load it all');
    expect(await getRetentionRules(request, datasourceName)).toEqual([
      expect.objectContaining({ type: 'loadForever', tieredReplicants: { _default_tier: 2 } }),
    ]);
    await expectRetentionInView(datasourcesOverview, datasourceName, 'loadForever(2x)');
    expect(await getUsedAndAvailableSegments(request, datasourceName)).toEqual([1, 1]);

    // dropForever (replacing loadForever): the Coordinator drops the segment, marking it unused, so the datasource is
    // only shown with "Show unused"
    await datasourcesOverview.setRetentionRules(datasourceName, ['dropForever'], 'Drop it all');
    expect(await getRetentionRules(request, datasourceName)).toEqual([{ type: 'dropForever' }]);
    await expect
      .poll(() => getUsedAndAvailableSegments(request, datasourceName), CLUSTER_STATE_POLL)
      .toEqual([0, 0]);
    await expect(async () => {
      expect(await datasourcesOverview.getDatasources(datasourceName)).toEqual([]);
      expect(
        await datasourcesOverview.getDatasources(datasourceName, { showUnused: true }),
      ).toEqual([expect.objectContaining({ availability: expect.stringContaining('Unused') })]);
    }).toPass({ timeout: 30 * 1000 });
  });
});

async function loadData(request: APIRequestContext, datasourceName: string) {
  const ingestionSpec = readTutorialIngestionSpec('wikipedia-index.json');
  ingestionSpec.spec.dataSchema.dataSource = datasourceName;
  await runTask(request, ingestionSpec);
  await expect
    .poll(() => getUsedAndAvailableSegments(request, datasourceName), CLUSTER_STATE_POLL)
    .toEqual([1, 1]);
}

async function getUsedAndAvailableSegments(request: APIRequestContext, datasourceName: string) {
  const { numSegments, numAvailableSegments } = await getDatasourceSegments(
    request,
    datasourceName,
  );
  return [numSegments, numAvailableSegments];
}

async function expectRetentionInView(
  datasourcesOverview: DatasourcesOverview,
  datasourceName: string,
  retention: string,
) {
  // Retried in case the view is a step behind the cluster state polled before
  await expect(async () => {
    const [datasource] = await datasourcesOverview.getDatasources(datasourceName);
    expect(datasource.retention).toContain(retention);
  }).toPass({ timeout: 30 * 1000 });
}
