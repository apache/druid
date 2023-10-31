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

import { sane } from '@druid-toolkit/query';

import { findAllSqlQueriesInText, findSqlQueryPrefix } from './sql';

describe('sql', () => {
  describe('getSqlQueryPrefix', () => {
    it('works when whole query parses', () => {
      expect(
        findSqlQueryPrefix(sane`
          SELECT *
          FROM wikipedia
        `),
      ).toMatchInlineSnapshot(`
        "SELECT *
        FROM wikipedia"
      `);
    });

    it('works when there are two queries', () => {
      expect(
        findSqlQueryPrefix(sane`
          SELECT *
          FROM wikipedia

          SELECT *
          FROM w2
        `),
      ).toMatchInlineSnapshot(`
        "SELECT *
        FROM wikipedia"
      `);
    });

    it('works when there are extra closing parens', () => {
      expect(
        findSqlQueryPrefix(sane`
          SELECT *
          FROM wikipedia)) lololol
        `),
      ).toMatchInlineSnapshot(`
        "SELECT *
        FROM wikipedia"
      `);
    });
  });

  describe('findAllSqlQueriesInText', () => {
    it('works with separate queries', () => {
      const text = sane`
        SELECT *
        FROM wikipedia

        SELECT *
        FROM w2
        LIMIT 5

        SELECT
      `;

      const found = findAllSqlQueriesInText(text);

      expect(found).toMatchInlineSnapshot(`
        Array [
          Object {
            "endOffset": 23,
            "endRowColumn": Object {
              "column": 14,
              "row": 1,
            },
            "sql": "SELECT *
        FROM wikipedia",
            "startOffset": 0,
            "startRowColumn": Object {
              "column": 0,
              "row": 0,
            },
          },
          Object {
            "endOffset": 49,
            "endRowColumn": Object {
              "column": 7,
              "row": 5,
            },
            "sql": "SELECT *
        FROM w2
        LIMIT 5",
            "startOffset": 25,
            "startRowColumn": Object {
              "column": 0,
              "row": 3,
            },
          },
        ]
      `);
    });

    it('works with simple query inside', () => {
      const text = sane`
        SELECT
          "channel",
          COUNT(*) AS "Count"
        FROM (SELECT * FROM "wikipedia")
        GROUP BY 1
        ORDER BY 2 DESC
      `;

      const found = findAllSqlQueriesInText(text);

      expect(found).toMatchInlineSnapshot(`
        Array [
          Object {
            "endOffset": 101,
            "endRowColumn": Object {
              "column": 15,
              "row": 5,
            },
            "sql": "SELECT
          \\"channel\\",
          COUNT(*) AS \\"Count\\"
        FROM (SELECT * FROM \\"wikipedia\\")
        GROUP BY 1
        ORDER BY 2 DESC",
            "startOffset": 0,
            "startRowColumn": Object {
              "column": 0,
              "row": 0,
            },
          },
          Object {
            "endOffset": 73,
            "endRowColumn": Object {
              "column": 31,
              "row": 3,
            },
            "sql": "SELECT * FROM \\"wikipedia\\"",
            "startOffset": 48,
            "startRowColumn": Object {
              "column": 6,
              "row": 3,
            },
          },
        ]
      `);
    });

    it('works with CTE query', () => {
      const text = sane`
        WITH w1 AS (
          SELECT channel, page FROM "wikipedia"
        )
        SELECT
          page,
          COUNT(*) AS "cnt"
        FROM w1
        GROUP BY 1
        ORDER BY 2 DESC
      `;

      const found = findAllSqlQueriesInText(text);

      expect(found).toMatchInlineSnapshot(`
        Array [
          Object {
            "endOffset": 124,
            "endRowColumn": Object {
              "column": 15,
              "row": 8,
            },
            "sql": "WITH w1 AS (
          SELECT channel, page FROM \\"wikipedia\\"
        )
        SELECT
          page,
          COUNT(*) AS \\"cnt\\"
        FROM w1
        GROUP BY 1
        ORDER BY 2 DESC",
            "startOffset": 0,
            "startRowColumn": Object {
              "column": 0,
              "row": 0,
            },
          },
          Object {
            "endOffset": 52,
            "endRowColumn": Object {
              "column": 39,
              "row": 1,
            },
            "sql": "SELECT channel, page FROM \\"wikipedia\\"",
            "startOffset": 15,
            "startRowColumn": Object {
              "column": 2,
              "row": 1,
            },
          },
          Object {
            "endOffset": 124,
            "endRowColumn": Object {
              "column": 15,
              "row": 8,
            },
            "sql": "SELECT
          page,
          COUNT(*) AS \\"cnt\\"
        FROM w1
        GROUP BY 1
        ORDER BY 2 DESC",
            "startOffset": 55,
            "startRowColumn": Object {
              "column": 0,
              "row": 3,
            },
          },
        ]
      `);
    });

    it('works with replace query', () => {
      const text = sane`
        REPLACE INTO "wikipedia" OVERWRITE ALL
        WITH "ext" AS (
          SELECT *
          FROM TABLE(
            EXTERN(
              '{"type":"http","uris":["https://druid.apache.org/data/wikipedia.json.gz"]}',
              '{"type":"json"}'
            )
          ) EXTEND ("isRobot" VARCHAR, "channel" VARCHAR, "timestamp" VARCHAR)
        )
        SELECT
          TIME_PARSE("timestamp") AS "__time",
          "isRobot",
          "channel"
        FROM "ext"
        PARTITIONED BY DAY
      `;

      const found = findAllSqlQueriesInText(text);

      expect(found).toMatchInlineSnapshot(`
        Array [
          Object {
            "endOffset": 379,
            "endRowColumn": Object {
              "column": 18,
              "row": 15,
            },
            "sql": "REPLACE INTO \\"wikipedia\\" OVERWRITE ALL
        WITH \\"ext\\" AS (
          SELECT *
          FROM TABLE(
            EXTERN(
              '{\\"type\\":\\"http\\",\\"uris\\":[\\"https://druid.apache.org/data/wikipedia.json.gz\\"]}',
              '{\\"type\\":\\"json\\"}'
            )
          ) EXTEND (\\"isRobot\\" VARCHAR, \\"channel\\" VARCHAR, \\"timestamp\\" VARCHAR)
        )
        SELECT
          TIME_PARSE(\\"timestamp\\") AS \\"__time\\",
          \\"isRobot\\",
          \\"channel\\"
        FROM \\"ext\\"
        PARTITIONED BY DAY",
            "startOffset": 0,
            "startRowColumn": Object {
              "column": 0,
              "row": 0,
            },
          },
          Object {
            "endOffset": 360,
            "endRowColumn": Object {
              "column": 10,
              "row": 14,
            },
            "sql": "WITH \\"ext\\" AS (
          SELECT *
          FROM TABLE(
            EXTERN(
              '{\\"type\\":\\"http\\",\\"uris\\":[\\"https://druid.apache.org/data/wikipedia.json.gz\\"]}',
              '{\\"type\\":\\"json\\"}'
            )
          ) EXTEND (\\"isRobot\\" VARCHAR, \\"channel\\" VARCHAR, \\"timestamp\\" VARCHAR)
        )
        SELECT
          TIME_PARSE(\\"timestamp\\") AS \\"__time\\",
          \\"isRobot\\",
          \\"channel\\"
        FROM \\"ext\\"",
            "startOffset": 39,
            "startRowColumn": Object {
              "column": 0,
              "row": 1,
            },
          },
          Object {
            "endOffset": 276,
            "endRowColumn": Object {
              "column": 70,
              "row": 8,
            },
            "sql": "SELECT *
          FROM TABLE(
            EXTERN(
              '{\\"type\\":\\"http\\",\\"uris\\":[\\"https://druid.apache.org/data/wikipedia.json.gz\\"]}',
              '{\\"type\\":\\"json\\"}'
            )
          ) EXTEND (\\"isRobot\\" VARCHAR, \\"channel\\" VARCHAR, \\"timestamp\\" VARCHAR)",
            "startOffset": 57,
            "startRowColumn": Object {
              "column": 2,
              "row": 2,
            },
          },
          Object {
            "endOffset": 360,
            "endRowColumn": Object {
              "column": 10,
              "row": 14,
            },
            "sql": "SELECT
          TIME_PARSE(\\"timestamp\\") AS \\"__time\\",
          \\"isRobot\\",
          \\"channel\\"
        FROM \\"ext\\"",
            "startOffset": 279,
            "startRowColumn": Object {
              "column": 0,
              "row": 10,
            },
          },
        ]
      `);
    });
  });
});
