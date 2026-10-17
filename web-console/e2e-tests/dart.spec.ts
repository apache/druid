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
import { T } from 'druid-query-toolkit';

import { CurrentDartPanel } from './component/workbench/dart-panel';
import { WorkbenchOverview } from './component/workbench/overview';
import { readTutorialIngestionSpec, runTask } from './util/druid';
import { expect, test } from './util/fixtures';
import { CLUSTER_STATE_POLL, getDartQueryState, getDatasourceSegments } from './util/sql';
import { showStep } from './util/steps';

const DART = { engine: 'SQL (Dart)' };

// Dart runs queries with the multi-stage query engine on the Broker and the Historicals (rather than in tasks), so it
// needs to be enabled (druid.msq.dart.enabled=true), as it is on the embedded clusters
test.describe('Dart', () => {
  test('Runs a query with Dart, which the current Dart queries panel shows with its details', async ({
    page,
    request,
    newDatasourceName,
  }) => {
    const datasourceName = newDatasourceName('dart');
    await loadData(request, datasourceName);

    const workbench = new WorkbenchOverview(page);
    const results = await workbench.runQuery(
      `SELECT channel, CAST(COUNT(*) AS VARCHAR) AS "count"
FROM ${T(datasourceName)}
GROUP BY 1
ORDER BY COUNT(*) DESC
LIMIT 2`,
      DART,
    );
    expect(results).toEqual([
      ['#en.wikipedia', '11549'],
      ['#vi.wikipedia', '9747'],
    ]);
    const { engine, sqlQueryId } = workbench.lastSubmittedQuery!;
    expect(engine).toBe('msq-dart');
    expect(await getDartQueryState(request, sqlQueryId!)).toBe('SUCCESS');

    const dartPanel = new CurrentDartPanel(page);
    await dartPanel.show();
    await expect
      .poll(() => dartPanel.getQueryState(sqlQueryId!), CLUSTER_STATE_POLL)
      .toBe('SUCCESS');
    await showStep(page, 'Current Dart queries panel: the query SUCCESS');

    // The details are the report of the query, with its stages
    const details = await dartPanel.showDetails(sqlQueryId!);
    await expect(details.locator('.execution-stages-pane .ct-tbody .ct-tr').first()).toBeVisible();
  });

  test('Cancels a running Dart query from the current Dart queries panel', async ({
    page,
    request,
    newDatasourceName,
  }) => {
    const datasourceName = newDatasourceName('dart-cancel');
    await loadData(request, datasourceName);

    // Sleeps for each row (10 ms, so minutes for all of them). The sleep depends on a column, as a constant one would
    // be done once, when the query is planned.
    const workbench = new WorkbenchOverview(page);
    const { sqlQueryId } = await workbench.startQuery(
      `SELECT COUNT(*)
FROM ${T(datasourceName)}
WHERE sleep(CASE WHEN "added" > 0 THEN 0.01 ELSE 0.02 END) IS NULL`,
      DART,
    );

    const dartPanel = new CurrentDartPanel(page);
    await dartPanel.show();
    await expect
      .poll(() => dartPanel.getQueryState(sqlQueryId!), CLUSTER_STATE_POLL)
      .toBe('RUNNING');
    await showStep(page, 'Current Dart queries panel: the query RUNNING');

    await dartPanel.cancelQuery(sqlQueryId!);
    await expect
      .poll(() => dartPanel.getQueryState(sqlQueryId!), CLUSTER_STATE_POLL)
      .toBe('CANCELED');
    await showStep(page, 'Current Dart queries panel: the query CANCELED');
    expect(await getDartQueryState(request, sqlQueryId!)).toBe('CANCELED');
  });
});

async function loadData(request: APIRequestContext, datasourceName: string) {
  const ingestionSpec = readTutorialIngestionSpec('wikipedia-index.json');
  ingestionSpec.spec.dataSchema.dataSource = datasourceName;
  await runTask(request, ingestionSpec);
  await expect
    .poll(() => getDatasourceSegments(request, datasourceName), CLUSTER_STATE_POLL)
    .toEqual({ numSegments: 1, numAvailableSegments: 1, numRows: 39244 });
}
