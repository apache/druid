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

import { loadData } from './component/load-data/data-loader';
import type { SupervisorAction } from './component/supervisors/overview';
import { SupervisorsOverview } from './component/supervisors/overview';
import { expect, test } from './util/fixtures';
import { CLUSTER_STATE_POLL, getQueryableRowCount, getSupervisorState } from './util/sql';

// A Kafka topic with the 39,244 Wikipedia edits of wikiticker-2015-09-12-sampled.json.gz (of
// examples/quickstart/tutorial), one per message, in one partition. KafkaWebConsoleTest (in embedded-tests) sets it
// up, with a Kafka container.
const BOOTSTRAP_SERVERS = process.env['DRUID_E2E_TEST_KAFKA_BOOTSTRAP_SERVERS'];
const TOPIC = process.env['DRUID_E2E_TEST_KAFKA_TOPIC'];

const NUM_ROWS = 39244;

// The workflow in this test is based on the Kafka tutorial:
// https://druid.apache.org/docs/latest/tutorials/tutorial-kafka
test.describe('Kafka ingestion', () => {
  test.skip(
    !BOOTSTRAP_SERVERS || !TOPIC,
    'needs DRUID_E2E_TEST_KAFKA_BOOTSTRAP_SERVERS and DRUID_E2E_TEST_KAFKA_TOPIC (run by KafkaWebConsoleTest in embedded-tests)',
  );

  test('Loads data from Kafka, then suspends, resumes and terminates the supervisor', async ({
    page,
    request,
    newDatasourceName,
  }) => {
    // The supervisor is named after the datasource
    const datasourceName = newDatasourceName('load-data-from-kafka');
    await loadData(page, {
      connector: { type: 'kafka', bootstrapServers: BOOTSTRAP_SERVERS!, topic: TOPIC! },
      validateConnect: lines => {
        expect(lines.length).toBeGreaterThan(0);
        // Each message is shown after its Kafka metadata ([ Kafka timestamp: ... ])
        expect(lines[0]).toContain('{"time":"2015-09-12T00:46:58.771Z","channel":"#en.wikipedia",');
      },
      rollup: false,
      segmentGranularity: 'day',
      datasourceName,
    });

    await expectSupervisorState(request, datasourceName, 'RUNNING');
    await expect
      .poll(() => getQueryableRowCount(request, datasourceName), CLUSTER_STATE_POLL)
      .toBe(NUM_ROWS);

    const supervisorsOverview = new SupervisorsOverview(page);
    await expectSupervisorInView(supervisorsOverview, datasourceName, 'RUNNING');

    await runSupervisorAction(page, request, datasourceName, 'Suspend', 'SUSPENDED');
    await runSupervisorAction(page, request, datasourceName, 'Resume', 'RUNNING');

    await supervisorsOverview.runAction(datasourceName, 'Terminate');
    await expectSupervisorState(request, datasourceName, null);
    expect(await supervisorsOverview.getSupervisors(datasourceName)).toEqual([]);

    // Terminating the supervisor publishes what its tasks ingested
    await expect
      .poll(() => getQueryableRowCount(request, datasourceName), CLUSTER_STATE_POLL)
      .toBe(NUM_ROWS);
  });
});

async function runSupervisorAction(
  page: Page,
  request: APIRequestContext,
  supervisorId: string,
  action: SupervisorAction,
  expectedState: string,
) {
  const supervisorsOverview = new SupervisorsOverview(page);
  await supervisorsOverview.runAction(supervisorId, action);
  await expectSupervisorState(request, supervisorId, expectedState);
  await expectSupervisorInView(supervisorsOverview, supervisorId, expectedState);
}

async function expectSupervisorState(
  request: APIRequestContext,
  supervisorId: string,
  state: string | null,
) {
  await expect
    .poll(() => getSupervisorState(request, supervisorId), CLUSTER_STATE_POLL)
    .toBe(state);
}

async function expectSupervisorInView(
  supervisorsOverview: SupervisorsOverview,
  supervisorId: string,
  status: string,
) {
  // Retried in case the view is a step behind the cluster state polled above
  await expect(async () => {
    const supervisors = await supervisorsOverview.getSupervisors(supervisorId);
    expect(supervisors).toHaveLength(1);
    expect(supervisors[0].status).toContain(status);
  }).toPass({ timeout: 30 * 1000 });
}
