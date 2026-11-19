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

import { loadData } from './component/load-data/data-loader';
import { expect, test } from './util/fixtures';
import { CLUSTER_STATE_POLL, getDatasourceSegments, getTaskStatuses } from './util/sql';

// The S3 URI of wikiticker-2015-09-12-sampled.json.gz (of examples/quickstart/tutorial), on an S3 that the cluster is
// configured for. S3WebConsoleTest (in embedded-tests) sets it up, with an S3 container.
const S3_URI = process.env['DRUID_E2E_TEST_S3_URI'];

test.describe('S3 ingestion', () => {
  test.skip(!S3_URI, 'needs DRUID_E2E_TEST_S3_URI (run by S3WebConsoleTest in embedded-tests)');

  test('Loads data from S3', async ({ page, request, newDatasourceName }) => {
    const datasourceName = newDatasourceName('load-data-from-s3');
    await loadData(page, {
      connector: { type: 's3', uris: [S3_URI!] },
      validateConnect: lines => {
        expect(lines.length).toBe(500);
        expect(lines[0]).toMatch(/^\{"time":"2015-09-12T00:46:58.771Z","channel":"#en.wikipedia",/);
      },
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
  });
});
