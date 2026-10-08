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

import type { APIRequestContext, Page } from '@playwright/test';

import { CompactionConfig } from './component/datasources/compaction';
import { DatasourcesOverview } from './component/datasources/overview';
import { HashedPartitionsSpec } from './component/load-data/config/partition';
import { readTutorialIngestionSpec, runIndexTask } from './util/druid';
import { expect, test } from './util/fixtures';
import { CLUSTER_STATE_POLL, getDatasourceSegments } from './util/sql';

// The workflow in these tests is based on the compaction tutorial:
// https://druid.apache.org/docs/latest/tutorials/tutorial-compaction.html
test.describe('Auto-compaction', () => {
  test('Compacts segments from dynamic to hash partitions', async ({ page, request }) => {
    const datasourceName = 'autocompaction-dynamic-to-hash' + new Date().toISOString();
    await loadInitialData(request, datasourceName);

    const numRows = 1412;
    await expect
      .poll(() => getDatasourceSegments(request, datasourceName), CLUSTER_STATE_POLL)
      .toEqual({ numSegments: 3, numAvailableSegments: 3, numRows });

    const compactionConfig = new CompactionConfig({
      skipOffsetFromLatest: 'PT0S',
      partitionsSpec: new HashedPartitionsSpec({
        numShards: null,
      }),
    });
    await configureCompaction(page, datasourceName, compactionConfig);

    // Depending on the number of configured tasks slots, autocompaction may
    // need several iterations if several time chunks need compaction
    const datasourcesOverview = new DatasourcesOverview(page);
    await expect(async () => {
      await datasourcesOverview.triggerCompaction();
      await expect
        .poll(() => getDatasourceSegments(request, datasourceName), {
          ...CLUSTER_STATE_POLL,
          timeout: 60 * 1000,
        })
        .toEqual({ numSegments: 2, numAvailableSegments: 2, numRows });
    }).toPass({ timeout: 4 * 60 * 1000 });
  });
});

async function loadInitialData(request: APIRequestContext, datasourceName: string) {
  const ingestionSpec = readTutorialIngestionSpec('compaction-init-index.json');
  const { dataSchema } = ingestionSpec.spec;
  dataSchema.dataSource = datasourceName;
  dataSchema.granularitySpec!.intervals = ['2015-09-12/2015-09-12T02:00']; // 2 hours rather than the day, to be faster
  await runIndexTask(request, ingestionSpec);
}

async function configureCompaction(
  page: Page,
  datasourceName: string,
  compactionConfig: CompactionConfig,
) {
  const datasourcesOverview = new DatasourcesOverview(page);
  await datasourcesOverview.setCompactionConfiguration(datasourceName, compactionConfig);

  // Saving the compaction config is not instantaneous
  await expect(async () => {
    const savedCompactionConfig =
      await datasourcesOverview.getCompactionConfiguration(datasourceName);
    expect(savedCompactionConfig).toEqual(compactionConfig);
  }).toPass();
}
