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

import { T } from 'druid-query-toolkit';

import { loadDataWithSql } from './component/load-data/sql-data-loader';
import { WorkbenchOverview } from './component/workbench/overview';
import { DRUID_EXAMPLES_QUICKSTART_TUTORIAL_DIR } from './util/druid';
import { expect, test } from './util/fixtures';
import { CLUSTER_STATE_POLL, getQueryableRowCount } from './util/sql';

const DATA_FILE = 'wikiticker-2015-09-12-sampled.json.gz';
const NUM_ROWS = 39244;
const NUM_EN_WIKIPEDIA_ROWS = 11549;

test.describe('SQL-based ingestion', () => {
  test('Loads data through the SQL data loader', async ({ page, request, newDatasourceName }) => {
    const datasourceName = newDatasourceName('sql-data-loader');
    await loadDataWithSql(page, {
      connector: {
        type: 'local',
        baseDirectory: DRUID_EXAMPLES_QUICKSTART_TUTORIAL_DIR,
        fileFilter: DATA_FILE,
      },
      validateParseData: ({ inputFormat, columns }) => {
        expect(inputFormat).toBe('json');
        expect(columns).toEqual(expect.arrayContaining(['time', 'channel', 'page', 'added']));
      },
      datasourceName,
    });

    await expect
      .poll(() => getQueryableRowCount(request, datasourceName), CLUSTER_STATE_POLL)
      .toBe(NUM_ROWS);
  });

  test('Ingests with REPLACE in the Query view, then reindexes with REPLACE from the datasource', async ({
    page,
    request,
    newDatasourceName,
  }) => {
    const workbench = new WorkbenchOverview(page);

    const datasourceName = newDatasourceName('sql-replace');
    const ingested = await workbench.runIngestQuery(`REPLACE INTO ${T(datasourceName)} OVERWRITE ALL
SELECT
  TIME_PARSE("time") AS __time,
  channel,
  page,
  added
FROM TABLE(
  EXTERN(
    '{"type":"local","filter":"${DATA_FILE}","baseDir":${JSON.stringify(
      DRUID_EXAMPLES_QUICKSTART_TUTORIAL_DIR,
    )}}',
    '{"type":"json"}'
  )
) EXTEND ("time" VARCHAR, channel VARCHAR, page VARCHAR, added BIGINT)
PARTITIONED BY DAY`);
    expect(ingested).toContain(`39,244 rows inserted into ${T(datasourceName)}`);
    await expect
      .poll(() => getQueryableRowCount(request, datasourceName), CLUSTER_STATE_POLL)
      .toBe(NUM_ROWS);

    // Reindex the rows of one channel into another datasource
    const reindexedName = newDatasourceName('sql-reindex');
    const reindexed = await workbench.runIngestQuery(`REPLACE INTO ${T(reindexedName)} OVERWRITE ALL
SELECT *
FROM ${T(datasourceName)}
WHERE channel = '#en.wikipedia'
PARTITIONED BY DAY`);
    expect(reindexed).toContain(`11,549 rows inserted into ${T(reindexedName)}`);
    await expect
      .poll(() => getQueryableRowCount(request, reindexedName), CLUSTER_STATE_POLL)
      .toBe(NUM_EN_WIKIPEDIA_ROWS);
  });
});
