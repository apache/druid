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

import type { SqlOrderByExpression } from 'druid-query-toolkit';
import { C, sql, SqlExpression, SqlQuery, T } from 'druid-query-toolkit';

import { nodePostJson } from '../../../test-utils';
import { ExpressionMeta, Measure } from '../models';

import type { CompareType, RestrictTop } from './table-query';
import { makeCompareMeasureName, makeTableQueryAndHints } from './table-query';
import type { Compare } from './time-manipulation';

const VALIDATE_QUERIES_WITH_DRUID = false;
const STOP_ON_FIRST_FAILURE = true;

if (VALIDATE_QUERIES_WITH_DRUID) {
  jest.setTimeout(600000);
}

function nlQueryToString(query: SqlQuery) {
  return `\n${query}\n`;
}

function batchArray<T>(array: T[], batchSize: number): T[][] {
  if (batchSize <= 0) {
    throw new Error('Batch size must be greater than 0');
  }

  const result: T[][] = [];
  for (let i = 0; i < array.length; i += batchSize) {
    result.push(array.slice(i, i + batchSize));
  }
  return result;
}

describe('table-query', () => {
  describe('specific tests', () => {
    const source = SqlQuery.create(T('kttm'));
    const where = sql`(TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -12) <= "__time" AND "__time" < TIMESTAMP '2019-08-26 00:00:00') AND "country" = 'United States'`;

    it('works with no split', () => {
      expect(
        nlQueryToString(
          makeTableQueryAndHints({
            source,
            where,
            splitColumns: [],
            showColumns: [],
            measures: [Measure.COUNT],
            maxRows: 200,
            orderBy: undefined,
          }).query,
        ),
      ).toMatchInlineSnapshot(`
        "
        SELECT COUNT(*) AS "Count"
        FROM (
          SELECT *
          FROM "kttm"
        ) AS "t"
        WHERE (TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -12) <= "__time" AND "__time" < TIMESTAMP '2019-08-26 00:00:00') AND "country" = 'United States'
        ORDER BY "Count" DESC
        LIMIT 200
        "
      `);
    });

    it('works with a non compare query', () => {
      expect(
        nlQueryToString(
          makeTableQueryAndHints({
            source,
            where,
            splitColumns: [new ExpressionMeta({ expression: C('browser_version'), as: 'Browser' })],
            showColumns: [new ExpressionMeta({ expression: C('browser'), as: 'Browser' })],
            measures: [
              Measure.COUNT,
              new Measure({
                expression: SqlExpression.parse('AVG("session_length")'),
                as: 'Avg. Session length',
              }),
            ],

            maxRows: 200,
            orderBy: undefined,
          }).query,
        ),
      ).toMatchInlineSnapshot(`
        "
        SELECT
          "browser_version" AS "Browser",
          CASE WHEN COUNT(DISTINCT "browser") = 1 THEN LATEST_BY("browser", "__time") ELSE NULL END AS "Browser",
          COUNT(*) AS "Count",
          AVG("session_length") AS "Avg. Session length"
        FROM (
          SELECT *
          FROM "kttm"
        ) AS "t"
        WHERE (TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -12) <= "__time" AND "__time" < TIMESTAMP '2019-08-26 00:00:00') AND "country" = 'United States'
        GROUP BY 1
        ORDER BY "Count" DESC
        LIMIT 200
        "
      `);
    });

    it('works with a single compare query (with restrict top)', () => {
      expect(
        nlQueryToString(
          makeTableQueryAndHints({
            source,
            where,
            splitColumns: [
              new ExpressionMeta({ expression: C('browser_version'), as: 'Browser Version' }),
            ],

            showColumns: [new ExpressionMeta({ expression: C('browser'), as: 'Browser' })],
            measures: [
              Measure.COUNT,
              new Measure({
                expression: SqlExpression.parse('AVG("session_length")'),
                as: 'Avg. Session length',
              }),
            ],

            compares: ['P1D'],
            compareStrategy: 'join',
            maxRows: 200,
            orderBy: undefined,
            useGroupingToOrderSubQueries: true,
          }).query,
        ),
      ).toMatchInlineSnapshot(`
        "
        WITH
        "common" AS (
          SELECT *
          FROM (
            SELECT *
            FROM "kttm"
          ) AS "t"
          WHERE ((TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -12) <= "t"."__time" AND "t"."__time" < TIMESTAMP '2019-08-26 00:00:00') OR (TIME_SHIFT(TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -12), 'P1D', -1) <= "t"."__time" AND "t"."__time" < TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'P1D', -1))) AND "t"."country" = 'United States'
        ),
        "top_values" AS (
          SELECT "t"."browser_version" AS "Browser Version"
          FROM "common" AS "t"
          GROUP BY 1
          ORDER BY COUNT(*) DESC
          LIMIT 5000
        ),
        "main" AS (
          SELECT
            "t"."browser_version" AS "Browser Version",
            CASE WHEN COUNT(DISTINCT "t"."browser") = 1 THEN LATEST_BY("t"."browser", "t"."__time") ELSE NULL END AS "Browser",
            COUNT(*) AS "Count",
            AVG("session_length") AS "Avg. Session length"
          FROM "common" AS "t"
          WHERE TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -12) <= "t"."__time" AND "t"."__time" < TIMESTAMP '2019-08-26 00:00:00'
          GROUP BY 1
          ORDER BY "Count" DESC
          LIMIT 200
        ),
        "compare_P1D" AS (
          SELECT
            "t"."browser_version" AS "Browser Version",
            CASE WHEN COUNT(DISTINCT "t"."browser") = 1 THEN LATEST_BY("t"."browser", "t"."__time") ELSE NULL END AS "Browser",
            COUNT(*) AS "Count",
            AVG("session_length") AS "Avg. Session length"
          FROM "common" AS "t"
          INNER JOIN "top_values" ON "t"."browser_version" IS NOT DISTINCT FROM "top_values"."Browser Version"
          WHERE TIME_SHIFT(TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -12), 'P1D', -1) <= "t"."__time" AND "t"."__time" < TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'P1D', -1)
          GROUP BY 1
        )
        SELECT
          COALESCE("main"."Browser Version", "compare_P1D"."Browser Version") AS "Browser Version",
          COALESCE(ANY_VALUE("main"."Browser"), ANY_VALUE("compare_P1D"."Browser")) AS "Browser",
          COALESCE(ANY_VALUE("main"."Count"), 0) AS "Count",
          COALESCE(ANY_VALUE("main"."Avg. Session length"), 0) AS "Avg. Session length",
          COALESCE(ANY_VALUE("compare_P1D"."Count"), 0) AS "Count:compare:P1D:value",
          COALESCE(ANY_VALUE("main"."Count"), 0) - COALESCE(ANY_VALUE("compare_P1D"."Count"), 0) AS "Count:compare:P1D:delta",
          COALESCE(ANY_VALUE("compare_P1D"."Avg. Session length"), 0) AS "Avg. Session length:compare:P1D:value",
          COALESCE(ANY_VALUE("main"."Avg. Session length"), 0) - COALESCE(ANY_VALUE("compare_P1D"."Avg. Session length"), 0) AS "Avg. Session length:compare:P1D:delta"
        FROM "main"
        FULL JOIN "compare_P1D" ON "main"."Browser Version" IS NOT DISTINCT FROM "compare_P1D"."Browser Version"
        GROUP BY 1
        ORDER BY "Count" DESC
        LIMIT 200
        "
      `);
    });

    it('works with a single compare query (with restrict top), order by delta', () => {
      expect(
        nlQueryToString(
          makeTableQueryAndHints({
            source,
            where,
            splitColumns: [
              new ExpressionMeta({ expression: C('browser_version'), as: 'Browser Version' }),
            ],
            showColumns: [new ExpressionMeta({ expression: C('browser'), as: 'Browser' })],
            measures: [
              Measure.COUNT,
              new Measure({
                expression: SqlExpression.parse('AVG("session_length")'),
                as: 'Avg. Session length',
              }),
            ],

            compares: ['P1D'],
            compareStrategy: 'join',
            maxRows: 200,
            orderBy: C(`Count:compare:P1D:delta`).toOrderByExpression('DESC'),
            useGroupingToOrderSubQueries: true,
          }).query,
        ),
      ).toMatchInlineSnapshot(`
        "
        WITH
        "common" AS (
          SELECT *
          FROM (
            SELECT *
            FROM "kttm"
          ) AS "t"
          WHERE ((TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -12) <= "t"."__time" AND "t"."__time" < TIMESTAMP '2019-08-26 00:00:00') OR (TIME_SHIFT(TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -12), 'P1D', -1) <= "t"."__time" AND "t"."__time" < TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'P1D', -1))) AND "t"."country" = 'United States'
        ),
        "top_values" AS (
          SELECT "t"."browser_version" AS "Browser Version"
          FROM "common" AS "t"
          GROUP BY 1
          ORDER BY COUNT(*) DESC
          LIMIT 5000
        ),
        "main" AS (
          SELECT
            "t"."browser_version" AS "Browser Version",
            CASE WHEN COUNT(DISTINCT "t"."browser") = 1 THEN LATEST_BY("t"."browser", "t"."__time") ELSE NULL END AS "Browser",
            COUNT(*) AS "Count",
            AVG("session_length") AS "Avg. Session length"
          FROM "common" AS "t"
          INNER JOIN "top_values" ON "t"."browser_version" IS NOT DISTINCT FROM "top_values"."Browser Version"
          WHERE TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -12) <= "t"."__time" AND "t"."__time" < TIMESTAMP '2019-08-26 00:00:00'
          GROUP BY 1
        ),
        "compare_P1D" AS (
          SELECT
            "t"."browser_version" AS "Browser Version",
            CASE WHEN COUNT(DISTINCT "t"."browser") = 1 THEN LATEST_BY("t"."browser", "t"."__time") ELSE NULL END AS "Browser",
            COUNT(*) AS "Count",
            AVG("session_length") AS "Avg. Session length"
          FROM "common" AS "t"
          INNER JOIN "top_values" ON "t"."browser_version" IS NOT DISTINCT FROM "top_values"."Browser Version"
          WHERE TIME_SHIFT(TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -12), 'P1D', -1) <= "t"."__time" AND "t"."__time" < TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'P1D', -1)
          GROUP BY 1
        )
        SELECT
          COALESCE("main"."Browser Version", "compare_P1D"."Browser Version") AS "Browser Version",
          COALESCE(ANY_VALUE("main"."Browser"), ANY_VALUE("compare_P1D"."Browser")) AS "Browser",
          COALESCE(ANY_VALUE("main"."Count"), 0) AS "Count",
          COALESCE(ANY_VALUE("main"."Avg. Session length"), 0) AS "Avg. Session length",
          COALESCE(ANY_VALUE("compare_P1D"."Count"), 0) AS "Count:compare:P1D:value",
          COALESCE(ANY_VALUE("main"."Count"), 0) - COALESCE(ANY_VALUE("compare_P1D"."Count"), 0) AS "Count:compare:P1D:delta",
          COALESCE(ANY_VALUE("compare_P1D"."Avg. Session length"), 0) AS "Avg. Session length:compare:P1D:value",
          COALESCE(ANY_VALUE("main"."Avg. Session length"), 0) - COALESCE(ANY_VALUE("compare_P1D"."Avg. Session length"), 0) AS "Avg. Session length:compare:P1D:delta"
        FROM "main"
        FULL JOIN "compare_P1D" ON "main"."Browser Version" IS NOT DISTINCT FROM "compare_P1D"."Browser Version"
        GROUP BY 1
        ORDER BY "Count:compare:P1D:delta" DESC
        LIMIT 200
        "
      `);
    });

    it('works with a single compare query (without restrict top)', () => {
      expect(
        nlQueryToString(
          makeTableQueryAndHints({
            source,
            where,
            splitColumns: [new ExpressionMeta({ expression: C('browser_version'), as: 'Browser' })],
            showColumns: [new ExpressionMeta({ expression: C('browser'), as: 'Browser' })],
            measures: [
              Measure.COUNT,
              new Measure({
                expression: SqlExpression.parse('AVG("session_length")'),
                as: 'Avg. Session length',
              }),
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
        "
        WITH
        "common" AS (
          SELECT *
          FROM (
            SELECT *
            FROM "kttm"
          ) AS "t"
          WHERE ((TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -12) <= "t"."__time" AND "t"."__time" < TIMESTAMP '2019-08-26 00:00:00') OR (TIME_SHIFT(TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -12), 'P1D', -1) <= "t"."__time" AND "t"."__time" < TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'P1D', -1))) AND "t"."country" = 'United States'
        ),
        "main" AS (
          SELECT
            "t"."browser_version" AS "Browser",
            CASE WHEN COUNT(DISTINCT "t"."browser") = 1 THEN LATEST_BY("t"."browser", "t"."__time") ELSE NULL END AS "Browser",
            COUNT(*) AS "Count",
            AVG("session_length") AS "Avg. Session length"
          FROM "common" AS "t"
          WHERE TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -12) <= "t"."__time" AND "t"."__time" < TIMESTAMP '2019-08-26 00:00:00'
          GROUP BY 1
          ORDER BY "Count" DESC
          LIMIT 200
        ),
        "compare_P1D" AS (
          SELECT
            "t"."browser_version" AS "Browser",
            CASE WHEN COUNT(DISTINCT "t"."browser") = 1 THEN LATEST_BY("t"."browser", "t"."__time") ELSE NULL END AS "Browser",
            COUNT(*) AS "Count",
            AVG("session_length") AS "Avg. Session length"
          FROM "common" AS "t"
          WHERE TIME_SHIFT(TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'PT1H', -12), 'P1D', -1) <= "t"."__time" AND "t"."__time" < TIME_SHIFT(TIMESTAMP '2019-08-26 00:00:00', 'P1D', -1)
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
        LIMIT 200
        "
      `);
    });
  });

  describe('uber test', () => {
    const source = SqlQuery.create(T('kttm'));
    const where = sql`(TIME_SHIFT(TIMESTAMP '2019-08-25 08:20:00', 'PT1H', -1) <= "__time" AND "__time" < TIMESTAMP '2019-08-25 14:20:00') AND "country" = 'United States'`;

    const splitColumnsVariations: [string, ExpressionMeta[]][] = [
      ['no group by', []],
      [
        'group by browser_version',
        [new ExpressionMeta({ expression: C('browser_version'), as: 'BrowserVersion' })],
      ],
      ['group by __time', [new ExpressionMeta({ expression: C('__time') })]],
      [
        'group by several things without time',
        [
          new ExpressionMeta({ expression: sql`LOWER(browser_version)`, as: 'BrowserVersion' }),
          new ExpressionMeta({ expression: C('country') }),
        ],
      ],
      [
        'group by several things and time',
        [
          new ExpressionMeta({ expression: C('__time'), as: 'Time' }),
          new ExpressionMeta({ expression: sql`LOWER(browser_version)`, as: 'BrowserVersion' }),
          new ExpressionMeta({ expression: C('country') }),
        ],
      ],
    ];

    const showColumnsVariations: [string, ExpressionMeta[]][] = [
      ['no show', []],
      ['show browser', [new ExpressionMeta({ expression: C('browser'), as: 'Browser' })]],
    ];

    const measuresVariations: [string, Measure[]][] = [
      ['no measures', []],
      ['one measure', [Measure.COUNT]],
      [
        'two measures',
        [
          Measure.COUNT,
          new Measure({
            expression: SqlExpression.parse('COUNT(DISTINCT "session")'),
            as: 'Unique sessions',
          }),
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
        for (const [measuresDescription, measures] of measuresVariations) {
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
                ...[...splitColumns, ...showColumns, ...measures].map(
                  ({ name }): [string, SqlOrderByExpression] => [
                    `order by ${name}`,
                    C(name).toOrderByExpression('DESC'),
                  ],
                ),
                ...compares.flatMap(compare =>
                  measures.flatMap(measure =>
                    compareTypes.map((compareType): [string, SqlOrderByExpression] => [
                      `${measure.name}/${compare}/${compareType}`,
                      C(
                        makeCompareMeasureName(measure.name, compare, compareType),
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
                      measuresDescription,
                      comparesDescription,
                      compareTypesDescription,
                      orderByDescription,
                      restrictTopDescription,
                    ]
                      .filter(Boolean)
                      .join(', '),
                    makeTableQueryAndHints({
                      source,
                      where,
                      splitColumns,
                      showColumns,
                      measures,

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

    console.log(`Generated ${descriptionAndQueries.length} queries`);

    it('all queries match snapshot', () => {
      batchArray(descriptionAndQueries, 100).forEach(descriptionAndQueriesBatch => {
        expect(
          descriptionAndQueriesBatch
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
