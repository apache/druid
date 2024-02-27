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
            "index": 0,
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
            "index": 1,
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
            "index": 0,
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
            "index": 1,
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
            "index": 0,
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
            "index": 1,
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
            "index": 2,
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

    it('works with select query followed by a replace query', () => {
      const text = sane`
        SELECT * FROM "wiki"

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
            "endOffset": 29,
            "endRowColumn": Object {
              "column": 7,
              "row": 2,
            },
            "index": 0,
            "sql": "SELECT * FROM \\"wiki\\"",
            "startOffset": 0,
            "startRowColumn": Object {
              "column": 0,
              "row": 0,
            },
          },
          Object {
            "endOffset": 401,
            "endRowColumn": Object {
              "column": 18,
              "row": 17,
            },
            "index": 1,
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
            "startOffset": 22,
            "startRowColumn": Object {
              "column": 0,
              "row": 2,
            },
          },
          Object {
            "endOffset": 382,
            "endRowColumn": Object {
              "column": 10,
              "row": 16,
            },
            "index": 2,
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
            "startOffset": 61,
            "startRowColumn": Object {
              "column": 0,
              "row": 3,
            },
          },
          Object {
            "endOffset": 298,
            "endRowColumn": Object {
              "column": 70,
              "row": 10,
            },
            "index": 3,
            "sql": "SELECT *
          FROM TABLE(
            EXTERN(
              '{\\"type\\":\\"http\\",\\"uris\\":[\\"https://druid.apache.org/data/wikipedia.json.gz\\"]}',
              '{\\"type\\":\\"json\\"}'
            )
          ) EXTEND (\\"isRobot\\" VARCHAR, \\"channel\\" VARCHAR, \\"timestamp\\" VARCHAR)",
            "startOffset": 79,
            "startRowColumn": Object {
              "column": 2,
              "row": 4,
            },
          },
          Object {
            "endOffset": 382,
            "endRowColumn": Object {
              "column": 10,
              "row": 16,
            },
            "index": 4,
            "sql": "SELECT
          TIME_PARSE(\\"timestamp\\") AS \\"__time\\",
          \\"isRobot\\",
          \\"channel\\"
        FROM \\"ext\\"",
            "startOffset": 301,
            "startRowColumn": Object {
              "column": 0,
              "row": 12,
            },
          },
        ]
      `);
    });

    it('works with explain plan query', () => {
      const text = sane`
        EXPLAIN PLAN FOR
        INSERT INTO "wikipedia"
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
        CLUSTERED BY "channel"
      `;

      const found = findAllSqlQueriesInText(text);

      expect(found).toMatchInlineSnapshot(`
        Array [
          Object {
            "endOffset": 404,
            "endRowColumn": Object {
              "column": 22,
              "row": 17,
            },
            "index": 0,
            "sql": "EXPLAIN PLAN FOR
        INSERT INTO \\"wikipedia\\"
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
        PARTITIONED BY DAY
        CLUSTERED BY \\"channel\\"",
            "startOffset": 0,
            "startRowColumn": Object {
              "column": 0,
              "row": 0,
            },
          },
          Object {
            "endOffset": 404,
            "endRowColumn": Object {
              "column": 22,
              "row": 17,
            },
            "index": 1,
            "sql": "INSERT INTO \\"wikipedia\\"
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
        PARTITIONED BY DAY
        CLUSTERED BY \\"channel\\"",
            "startOffset": 17,
            "startRowColumn": Object {
              "column": 0,
              "row": 1,
            },
          },
          Object {
            "endOffset": 362,
            "endRowColumn": Object {
              "column": 10,
              "row": 15,
            },
            "index": 2,
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
            "startOffset": 41,
            "startRowColumn": Object {
              "column": 0,
              "row": 2,
            },
          },
          Object {
            "endOffset": 278,
            "endRowColumn": Object {
              "column": 70,
              "row": 9,
            },
            "index": 3,
            "sql": "SELECT *
          FROM TABLE(
            EXTERN(
              '{\\"type\\":\\"http\\",\\"uris\\":[\\"https://druid.apache.org/data/wikipedia.json.gz\\"]}',
              '{\\"type\\":\\"json\\"}'
            )
          ) EXTEND (\\"isRobot\\" VARCHAR, \\"channel\\" VARCHAR, \\"timestamp\\" VARCHAR)",
            "startOffset": 59,
            "startRowColumn": Object {
              "column": 2,
              "row": 3,
            },
          },
          Object {
            "endOffset": 362,
            "endRowColumn": Object {
              "column": 10,
              "row": 15,
            },
            "index": 4,
            "sql": "SELECT
          TIME_PARSE(\\"timestamp\\") AS \\"__time\\",
          \\"isRobot\\",
          \\"channel\\"
        FROM \\"ext\\"",
            "startOffset": 281,
            "startRowColumn": Object {
              "column": 0,
              "row": 11,
            },
          },
        ]
      `);
    });

    it('works with multiple explain plan queries', () => {
      const text = sane`
        EXPLAIN PLAN FOR
        SELECT *
        FROM wikipedia

        EXPLAIN PLAN FOR
        SELECT *
        FROM w2
        LIMIT 5

      `;

      const found = findAllSqlQueriesInText(text);

      expect(found).toMatchInlineSnapshot(`
        Array [
          Object {
            "endOffset": 40,
            "endRowColumn": Object {
              "column": 14,
              "row": 2,
            },
            "index": 0,
            "sql": "EXPLAIN PLAN FOR
        SELECT *
        FROM wikipedia",
            "startOffset": 0,
            "startRowColumn": Object {
              "column": 0,
              "row": 0,
            },
          },
          Object {
            "endOffset": 40,
            "endRowColumn": Object {
              "column": 14,
              "row": 2,
            },
            "index": 1,
            "sql": "SELECT *
        FROM wikipedia",
            "startOffset": 17,
            "startRowColumn": Object {
              "column": 0,
              "row": 1,
            },
          },
          Object {
            "endOffset": 83,
            "endRowColumn": Object {
              "column": 7,
              "row": 7,
            },
            "index": 2,
            "sql": "EXPLAIN PLAN FOR
        SELECT *
        FROM w2
        LIMIT 5",
            "startOffset": 42,
            "startRowColumn": Object {
              "column": 0,
              "row": 4,
            },
          },
          Object {
            "endOffset": 83,
            "endRowColumn": Object {
              "column": 7,
              "row": 7,
            },
            "index": 3,
            "sql": "SELECT *
        FROM w2
        LIMIT 5",
            "startOffset": 59,
            "startRowColumn": Object {
              "column": 0,
              "row": 5,
            },
          },
        ]
      `);
    });
  });
});
