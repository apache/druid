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

import { sane, SqlQuery } from '@druid-toolkit/query';

import { Measure } from '../models';

import { rewriteAggregate } from './aggregate';

describe('rewriteAggregate', () => {
  const measures = Measure.extractQueryMeasures(
    SqlQuery.parse(sane`
      SELECT
        t."time" AS "Time",
        t."host" AS "Host"
        --:MEASURE SUM(CASE WHEN (t."metric"='query/time') THEN t."count" ELSE 0 END) AS "Queries"
      FROM "metrics" AS "t"
    `),
  );

  it('works with ORDER BY', () => {
    expect(
      rewriteAggregate(
        SqlQuery.parse(sane`
          SELECT "Time" AS "v"
          FROM (
            SELECT
              t."time" AS "Time",
              t."host" AS "Host"
              --:MEASURE SUM(CASE WHEN (t."metric"='query/time') THEN t."count" ELSE 0 END) AS "Queries"
            FROM "metrics" AS "t"
          ) AS "t"
          WHERE (TIME_SHIFT(TIMESTAMP '2024-10-03 19:03:45.332', 'P1D', -1) <= "Time" AND "Time" < TIMESTAMP '2024-10-03 19:03:45.332')
          GROUP BY 1
          ORDER BY AGGREGATE('Queries') DESC
          LIMIT 7
          `),
        measures,
      ),
    ).toMatchInlineSnapshot(`
      "SELECT "Time" AS "v"
      FROM (
        SELECT
          t."time" AS "Time",
          t."host" AS "Host",
          "count",
          "metric"
          --:MEASURE SUM(CASE WHEN (t."metric"='query/time') THEN t."count" ELSE 0 END) AS "Queries"
        FROM "metrics" AS "t"
      ) AS "t"
      WHERE (TIME_SHIFT(TIMESTAMP '2024-10-03 19:03:45.332', 'P1D', -1) <= "Time" AND "Time" < TIMESTAMP '2024-10-03 19:03:45.332')
      GROUP BY 1
      ORDER BY SUM(CASE WHEN (t."metric"='query/time') THEN t."count" ELSE 0 END) DESC
      LIMIT 7"
    `);
  });
});
