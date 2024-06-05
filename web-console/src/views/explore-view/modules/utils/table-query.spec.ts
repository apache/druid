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

import type { SqlOrderByExpression, SqlQuery } from '@druid-toolkit/query';
import { C, sql, SqlExpression, T } from '@druid-toolkit/query';
import type { ExpressionMeta } from '@druid-toolkit/visuals-core';

import { nodePostJson } from '../../../../test-utils';

import type { CompareType, RestrictTop } from './table-query';
import { makeCompareMetricName, makeTableQueryAndHints } from './table-query';
import type { Compare } from './utils';

const VALIDATE_QUERIES_WITH_DRUID = false;
const STOP_ON_FIRST_FAILURE = true;

if (VALIDATE_QUERIES_WITH_DRUID) {
  jest.setTimeout(600000);
}

describe('table-query', () => {
  describe('specific tests', () => {
    const table = T('kttm');
    const where = SqlExpression.parse(
      `(TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -1) <= "t"."__time" AND "t"."__time" < TIMESTAMP '2019-08-26 00:00:00') AND "country" = 'United States'`,
    );

    it('works with no split', () => {
      expect(
        String(
          makeTableQueryAndHints({
            table,
            where,
            splitColumns: [],
            showColumns: [],
            metrics: [{ expression: SqlExpression.parse('COUNT(*)'), name: 'Count' }],
            maxRows: 200,
            orderBy: undefined,
          }).query,
        ),
      ).toMatchInlineSnapshot(`
        "SELECT COUNT(*) AS "Count"
        FROM "kttm" AS "t"
        WHERE (TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -1) <= "t"."__time" AND "t"."__time" < TIMESTAMP '2019-08-26 00:00:00') AND "t"."country" = 'United States'
        ORDER BY "Count" DESC
        LIMIT 200"
    `);
    });

    it('works with a non compare query', () => {
      expect(
        String(
          makeTableQueryAndHints({
            table,
            where,
            splitColumns: [{ expression: C('browser_version'), name: 'Browser' }],
            showColumns: [{ expression: C('browser'), name: 'Browser' }],
            metrics: [
              { expression: SqlExpression.parse('COUNT(*)'), name: 'Count' },
              {
                expression: SqlExpression.parse('AVG("session_length")'),
                name: 'Avg. Session length',
              },
            ],

            maxRows: 200,
            orderBy: undefined,
          }).query,
        ),
      ).toMatchInlineSnapshot(`
        "SELECT
          "t"."browser_version" AS "Browser",
          CASE WHEN COUNT(DISTINCT "browser") = 1 THEN LATEST("browser") ELSE NULL END AS "Browser",
          COUNT(*) AS "Count",
          AVG("session_length") AS "Avg. Session length"
        FROM "kttm" AS "t"
        WHERE (TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -1) <= "t"."__time" AND "t"."__time" < TIMESTAMP '2019-08-26 00:00:00') AND "t"."country" = 'United States'
        GROUP BY 1
        ORDER BY "Count" DESC
        LIMIT 200"
    `);
    });

    it('works with a single compare query (with restrict top)', () => {
      expect(
        String(
          makeTableQueryAndHints({
            table,
            where,
            splitColumns: [{ expression: C('browser_version'), name: 'Browser' }],
            showColumns: [{ expression: C('browser'), name: 'Browser' }],
            metrics: [
              { expression: SqlExpression.parse('COUNT(*)'), name: 'Count' },
              {
                expression: SqlExpression.parse('AVG("session_length")'),
                name: 'Avg. Session length',
              },
            ],

            compares: ['P1D'],
            compareStrategy: 'join',
            maxRows: 200,
            orderBy: undefined,
            useGroupingToOrderSubQueries: true,
          }).query,
        ),
      ).toMatchInlineSnapshot(`
        "WITH
        "top_values" AS (
          SELECT "t"."browser_version" AS "Browser"
          FROM "kttm" AS "t"
          WHERE ((TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -1) <= "t"."__time" AND "t"."__time" < TIMESTAMP '2019-08-26 00:00:00') OR (TIME_SHIFT(TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -1), 'P1D', -1) <= "t"."__time" AND "t"."__time" < TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'P1D', -1))) AND "t"."country" = 'United States'
          GROUP BY 1
          ORDER BY COUNT(*) DESC
          LIMIT 5000
        ),
        "main" AS (
          SELECT
            "t"."browser_version" AS "Browser",
            CASE WHEN COUNT(DISTINCT "browser") = 1 THEN LATEST("browser") ELSE NULL END AS "Browser",
            COUNT(*) AS "Count",
            AVG("session_length") AS "Avg. Session length"
          FROM "kttm" AS "t"
          WHERE (TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -1) <= "t"."__time" AND "t"."__time" < TIMESTAMP '2019-08-26 00:00:00') AND "t"."country" = 'United States'
          GROUP BY 1
          ORDER BY "Count" DESC
          LIMIT 200
        ),
        "compare_P1D" AS (
          SELECT
            "t"."browser_version" AS "Browser",
            CASE WHEN COUNT(DISTINCT "browser") = 1 THEN LATEST("browser") ELSE NULL END AS "Browser",
            COUNT(*) AS "Count",
            AVG("session_length") AS "Avg. Session length"
          FROM "kttm" AS "t"
          INNER JOIN "top_values" ON "t"."browser_version" IS NOT DISTINCT FROM "top_values"."Browser"
          WHERE (TIME_SHIFT(TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -1), 'P1D', -1) <= "t"."__time" AND "t"."__time" < TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'P1D', -1)) AND "t"."country" = 'United States'
          GROUP BY 1
        )
        SELECT
          COALESCE("main"."Browser", "compare_P1D"."Browser") AS "Browser",
          COALESCE(ANY_VALUE("main"."Browser"), ANY_VALUE("compare_P1D"."Browser")) AS "Browser",
          COALESCE(ANY_VALUE("main"."Count"), 0) AS "Count",
          COALESCE(ANY_VALUE("main"."Avg. Session length"), 0) AS "Avg. Session length",
          COALESCE(ANY_VALUE("compare_P1D"."Count"), 0) AS "Count:compare:P1D:value",
          COALESCE(ANY_VALUE("main"."Count"), 0) - COALESCE(ANY_VALUE("compare_P1D"."Count"), 0) AS "Count:compare:P1D:delta",
          COALESCE(ANY_VALUE("compare_P1D"."Avg. Session length"), 0) AS "Avg. Session length:compare:P1D:value",
          COALESCE(ANY_VALUE("main"."Avg. Session length"), 0) - COALESCE(ANY_VALUE("compare_P1D"."Avg. Session length"), 0) AS "Avg. Session length:compare:P1D:delta"
        FROM "main"
        FULL JOIN "compare_P1D" ON "main"."Browser" IS NOT DISTINCT FROM "compare_P1D"."Browser"
        GROUP BY 1
        ORDER BY "Count" DESC
        LIMIT 200"
      `);
    });

    it('works with a single compare query (with restrict top), order by delta', () => {
      expect(
        String(
          makeTableQueryAndHints({
            table,
            where,
            splitColumns: [{ expression: C('browser_version'), name: 'Browser' }],
            showColumns: [{ expression: C('browser'), name: 'Browser' }],
            metrics: [
              { expression: SqlExpression.parse('COUNT(*)'), name: 'Count' },
              {
                expression: SqlExpression.parse('AVG("session_length")'),
                name: 'Avg. Session length',
              },
            ],

            compares: ['P1D'],
            compareStrategy: 'join',
            maxRows: 200,
            orderBy: C(`Count:compare:P1D:delta`).toOrderByExpression('DESC'),
            useGroupingToOrderSubQueries: true,
          }).query,
        ),
      ).toMatchInlineSnapshot(`
        "WITH
        "top_values" AS (
          SELECT "t"."browser_version" AS "Browser"
          FROM "kttm" AS "t"
          WHERE ((TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -1) <= "t"."__time" AND "t"."__time" < TIMESTAMP '2019-08-26 00:00:00') OR (TIME_SHIFT(TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -1), 'P1D', -1) <= "t"."__time" AND "t"."__time" < TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'P1D', -1))) AND "t"."country" = 'United States'
          GROUP BY 1
          ORDER BY COUNT(*) DESC
          LIMIT 5000
        ),
        "main" AS (
          SELECT
            "t"."browser_version" AS "Browser",
            CASE WHEN COUNT(DISTINCT "browser") = 1 THEN LATEST("browser") ELSE NULL END AS "Browser",
            COUNT(*) AS "Count",
            AVG("session_length") AS "Avg. Session length"
          FROM "kttm" AS "t"
          INNER JOIN "top_values" ON "t"."browser_version" IS NOT DISTINCT FROM "top_values"."Browser"
          WHERE (TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -1) <= "t"."__time" AND "t"."__time" < TIMESTAMP '2019-08-26 00:00:00') AND "t"."country" = 'United States'
          GROUP BY 1
        ),
        "compare_P1D" AS (
          SELECT
            "t"."browser_version" AS "Browser",
            CASE WHEN COUNT(DISTINCT "browser") = 1 THEN LATEST("browser") ELSE NULL END AS "Browser",
            COUNT(*) AS "Count",
            AVG("session_length") AS "Avg. Session length"
          FROM "kttm" AS "t"
          INNER JOIN "top_values" ON "t"."browser_version" IS NOT DISTINCT FROM "top_values"."Browser"
          WHERE (TIME_SHIFT(TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -1), 'P1D', -1) <= "t"."__time" AND "t"."__time" < TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'P1D', -1)) AND "t"."country" = 'United States'
          GROUP BY 1
        )
        SELECT
          COALESCE("main"."Browser", "compare_P1D"."Browser") AS "Browser",
          COALESCE(ANY_VALUE("main"."Browser"), ANY_VALUE("compare_P1D"."Browser")) AS "Browser",
          COALESCE(ANY_VALUE("main"."Count"), 0) AS "Count",
          COALESCE(ANY_VALUE("main"."Avg. Session length"), 0) AS "Avg. Session length",
          COALESCE(ANY_VALUE("compare_P1D"."Count"), 0) AS "Count:compare:P1D:value",
          COALESCE(ANY_VALUE("main"."Count"), 0) - COALESCE(ANY_VALUE("compare_P1D"."Count"), 0) AS "Count:compare:P1D:delta",
          COALESCE(ANY_VALUE("compare_P1D"."Avg. Session length"), 0) AS "Avg. Session length:compare:P1D:value",
          COALESCE(ANY_VALUE("main"."Avg. Session length"), 0) - COALESCE(ANY_VALUE("compare_P1D"."Avg. Session length"), 0) AS "Avg. Session length:compare:P1D:delta"
        FROM "main"
        FULL JOIN "compare_P1D" ON "main"."Browser" IS NOT DISTINCT FROM "compare_P1D"."Browser"
        GROUP BY 1
        ORDER BY "Count:compare:P1D:delta" DESC
        LIMIT 200"
      `);
    });

    it('works with a single compare query (without restrict top)', () => {
      expect(
        String(
          makeTableQueryAndHints({
            table,
            where,
            splitColumns: [{ expression: C('browser_version'), name: 'Browser' }],
            showColumns: [{ expression: C('browser'), name: 'Browser' }],
            metrics: [
              { expression: SqlExpression.parse('COUNT(*)'), name: 'Count' },
              {
                expression: SqlExpression.parse('AVG("session_length")'),
                name: 'Avg. Session length',
              },
            ],

            compares: ['P1D'],
            compareStrategy: 'join',
            maxRows: 200,
            restrictTop: 'never',
            orderBy: undefined,
            useGroupingToOrderSubQueries: true,
          }).query,
        ),
      ).toMatchInlineSnapshot(`
        "WITH
        "main" AS (
          SELECT
            "t"."browser_version" AS "Browser",
            CASE WHEN COUNT(DISTINCT "browser") = 1 THEN LATEST("browser") ELSE NULL END AS "Browser",
            COUNT(*) AS "Count",
            AVG("session_length") AS "Avg. Session length"
          FROM "kttm" AS "t"
          WHERE (TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -1) <= "t"."__time" AND "t"."__time" < TIMESTAMP '2019-08-26 00:00:00') AND "t"."country" = 'United States'
          GROUP BY 1
          ORDER BY "Count" DESC
          LIMIT 200
        ),
        "compare_P1D" AS (
          SELECT
            "t"."browser_version" AS "Browser",
            CASE WHEN COUNT(DISTINCT "browser") = 1 THEN LATEST("browser") ELSE NULL END AS "Browser",
            COUNT(*) AS "Count",
            AVG("session_length") AS "Avg. Session length"
          FROM "kttm" AS "t"
          WHERE (TIME_SHIFT(TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -1), 'P1D', -1) <= "t"."__time" AND "t"."__time" < TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'P1D', -1)) AND "t"."country" = 'United States'
          GROUP BY 1
          ORDER BY "Count" DESC
          LIMIT 50000
        )
        SELECT
          COALESCE("main"."Browser", "compare_P1D"."Browser") AS "Browser",
          COALESCE(ANY_VALUE("main"."Browser"), ANY_VALUE("compare_P1D"."Browser")) AS "Browser",
          COALESCE(ANY_VALUE("main"."Count"), 0) AS "Count",
          COALESCE(ANY_VALUE("main"."Avg. Session length"), 0) AS "Avg. Session length",
          COALESCE(ANY_VALUE("compare_P1D"."Count"), 0) AS "Count:compare:P1D:value",
          COALESCE(ANY_VALUE("main"."Count"), 0) - COALESCE(ANY_VALUE("compare_P1D"."Count"), 0) AS "Count:compare:P1D:delta",
          COALESCE(ANY_VALUE("compare_P1D"."Avg. Session length"), 0) AS "Avg. Session length:compare:P1D:value",
          COALESCE(ANY_VALUE("main"."Avg. Session length"), 0) - COALESCE(ANY_VALUE("compare_P1D"."Avg. Session length"), 0) AS "Avg. Session length:compare:P1D:delta"
        FROM "main"
        FULL JOIN "compare_P1D" ON "main"."Browser" IS NOT DISTINCT FROM "compare_P1D"."Browser"
        GROUP BY 1
        ORDER BY "Count" DESC
        LIMIT 200"
      `);
    });
  });

  describe('uber test', () => {
    const table = T('kttm');
    const where = sql`(TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -1) <= "t"."__time" AND "t"."__time" < TIMESTAMP '2019-08-26 00:00:00') AND "country" = 'United States'`;

    const splitColumnsVariations: [string, ExpressionMeta[]][] = [
      ['no group by', []],
      ['group by browser_version', [{ expression: C('browser_version'), name: 'BrowserVersion' }]],
      ['group by time', [{ expression: C('__time'), name: 'Time', sqlType: 'TIMESTAMP' }]],
      [
        'group by several things without time',
        [
          { expression: sql`LOWER(browser_version)`, name: 'BrowserVersion' },
          { expression: C('country'), name: 'country' },
        ],
      ],
      [
        'group by several things and time',
        [
          { expression: C('__time'), name: 'Time', sqlType: 'TIMESTAMP' },
          { expression: sql`LOWER(browser_version)`, name: 'BrowserVersion' },
          { expression: C('country'), name: 'country' },
        ],
      ],
    ];

    const showColumnsVariations: [string, ExpressionMeta[]][] = [
      ['no show', []],
      ['show browser', [{ expression: C('browser'), name: 'Browser' }]],
    ];

    const metricsVariations: [string, ExpressionMeta[]][] = [
      ['no metrics', []],
      ['one metric', [{ expression: SqlExpression.parse('COUNT(*)'), name: 'Count' }]],
      [
        'two metrics',
        [
          { expression: SqlExpression.parse('COUNT(*)'), name: 'Count' },
          { expression: SqlExpression.parse('COUNT(DISTINCT "session")'), name: 'Unique sessions' },
        ],
      ],
    ];

    const comparesVariations: [string, Compare[]][] = [
      ['no compare', []],
      ['compare PT1H', ['PT1H']],
    ];

    const descriptionAndQueries: [string, SqlQuery][] = [];
    for (const [splitColumnsDescription, splitColumns] of splitColumnsVariations) {
      for (const [showColumnsDescription, showColumns] of showColumnsVariations) {
        for (const [metricsDescription, metrics] of metricsVariations) {
          for (const [comparesDescription, compares] of comparesVariations) {
            const compareTypesVariations: [string, CompareType[]][] = compares.length
              ? [
                  ['value+delta', ['value', 'delta']],
                  ['delta+percent', ['delta', 'percent']],
                  ['all compare types', ['value', 'delta', 'absDelta', 'percent', 'absPercent']],
                ]
              : [['', ['value', 'delta']]];

            const restrictTopVariations: [string, RestrictTop][] = compares.length
              ? [
                  ['restrict never', 'never'],
                  ['restrict always', 'always'],
                ]
              : [['', 'never']];

            for (const [compareTypesDescription, compareTypes] of compareTypesVariations) {
              const orderByVariations: [string, SqlOrderByExpression | undefined][] = [
                ['no order by', undefined],
                ...[...splitColumns, ...showColumns, ...metrics].map(
                  ({ name }): [string, SqlOrderByExpression] => [
                    `order by ${name}`,
                    C(name).toOrderByExpression('DESC'),
                  ],
                ),
                ...compares.flatMap(compare =>
                  metrics.flatMap(metric =>
                    compareTypes.map((compareType): [string, SqlOrderByExpression] => [
                      `${metric.name}/${compare}/${compareType}`,
                      C(
                        makeCompareMetricName(metric.name, compare, compareType),
                      ).toOrderByExpression('DESC'),
                    ]),
                  ),
                ),
              ];

              for (const [orderByDescription, orderBy] of orderByVariations) {
                for (const [restrictTopDescription, restrictTop] of restrictTopVariations) {
                  descriptionAndQueries.push([
                    [
                      splitColumnsDescription,
                      showColumnsDescription,
                      metricsDescription,
                      comparesDescription,
                      compareTypesDescription,
                      orderByDescription,
                      restrictTopDescription,
                    ]
                      .filter(Boolean)
                      .join(', '),
                    makeTableQueryAndHints({
                      table,
                      where,
                      splitColumns,
                      showColumns,
                      metrics,

                      compares,
                      compareStrategy: 'auto',
                      compareTypes,
                      maxRows: 200,
                      orderBy,
                      restrictTop,
                      useGroupingToOrderSubQueries: true,
                    }).query,
                  ]);
                }
              }
            }
          }
        }
      }
    }

    it('all queries match snapshot', () => {
      expect(
        descriptionAndQueries
          .flatMap(([description, query]) => {
            return [
              '',
              '===========================================',
              description,
              '-----',
              query.toString(),
            ];
          })
          .join('\n'),
      ).toMatchSnapshot();
    });

    it('validates with Druid', async () => {
      if (!VALIDATE_QUERIES_WITH_DRUID) return;

      const failedDescriptions: string[] = [];
      for (let i = 0; i < descriptionAndQueries.length; i++) {
        console.log(`Running query ${i + 1} of ${descriptionAndQueries.length}`);
        const [description, query] = descriptionAndQueries[i];
        try {
          await nodePostJson('http://localhost:8888/druid/v2/sql', {
            query: query.toString(),
            context: { sqlOuterLimit: 500 },
          });
        } catch (e) {
          console.log(`Failed on: ${e.message.replace(/\n|\r/g, '')}`);
          console.log(query.toString());
          if (STOP_ON_FIRST_FAILURE) {
            throw e;
          } else {
            failedDescriptions.push(description);
          }
        }
      }

      if (failedDescriptions.length) {
        console.log(
          `Failed on ${failedDescriptions.length} queries: \n${failedDescriptions.join('\n')}`,
        );
      }

      expect(failedDescriptions.length).toEqual(0);
    });
  });
});
