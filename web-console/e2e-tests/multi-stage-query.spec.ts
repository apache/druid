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

import { WorkbenchOverview } from './component/workbench/overview';
import { DRUID_EXAMPLES_QUICKSTART_TUTORIAL_DIR } from './util/druid';
import { expect, test } from './util/fixtures';

test.describe('Multi-stage query', () => {
  test('runs a query that reads external data', async ({ page }) => {
    const workbench = new WorkbenchOverview(page);

    const results = await workbench.runQuery(`WITH ext AS (SELECT *
FROM TABLE(
  EXTERN(
    '{"type":"local","filter":"wikiticker-2015-09-12-sampled.json.gz","baseDir":${JSON.stringify(
      DRUID_EXAMPLES_QUICKSTART_TUTORIAL_DIR,
    )}}',
    '{"type":"json"}'
  )
) EXTEND (channel VARCHAR))
SELECT
  channel,
  CAST(COUNT(*) AS VARCHAR) AS "CountString"
FROM ext
GROUP BY 1
ORDER BY COUNT(*) DESC
LIMIT 10`);
    expect(results.length).toBe(10);
    expect(results[0]).toStrictEqual(['#en.wikipedia', '11549']);
    expect(results[1]).toStrictEqual(['#vi.wikipedia', '9747']);
  });
});
