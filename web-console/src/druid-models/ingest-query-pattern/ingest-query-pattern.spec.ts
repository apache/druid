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

import { fitIngestQueryPattern, ingestQueryPatternToQuery } from './ingest-query-pattern';

describe('ingest-query-pattern', () => {
  it('works with no group by', () => {
    const query = SqlQuery.parse(sane`
      INSERT INTO "kttm-2019"
      WITH "ext" AS (
        SELECT *
        FROM TABLE(
          EXTERN(
            '{"type":"http","uris":["https://example.com/data.json.gz"]}',
            '{"type":"json"}'
          )
        ) EXTEND ("timestamp" VARCHAR, "agent_category" VARCHAR, "agent_type" VARCHAR, "browser" VARCHAR, "browser_version" VARCHAR, "city" VARCHAR, "continent" VARCHAR, "country" VARCHAR, "version" VARCHAR, "event_type" VARCHAR, "event_subtype" VARCHAR, "loaded_image" VARCHAR, "adblock_list" VARCHAR, "forwarded_for" VARCHAR, "language" VARCHAR, "number" BIGINT, "os" VARCHAR, "path" VARCHAR, "platform" VARCHAR, "referrer" VARCHAR, "referrer_host" VARCHAR, "region" VARCHAR, "remote_address" VARCHAR, "screen" VARCHAR, "session" VARCHAR, "session_length" BIGINT, "timezone" VARCHAR, "timezone_offset" BIGINT, "window" VARCHAR)
      )
      SELECT
        TIME_PARSE("timestamp") AS __time,
        agent_category,
        agent_type,
        browser,
        browser_version
      FROM "ext"
      PARTITIONED BY HOUR
      CLUSTERED BY 4
    `);

    const insertQueryPattern = fitIngestQueryPattern(query);

    expect(insertQueryPattern).toMatchInlineSnapshot(`
      Object {
        "clusteredBy": Array [
          3,
        ],
        "destinationTableName": "kttm-2019",
        "dimensions": Array [
          "TIME_PARSE(\\"timestamp\\") AS __time",
          "agent_category",
          "agent_type",
          "browser",
          "browser_version",
        ],
        "filters": Array [],
        "mainExternalConfig": Object {
          "inputFormat": Object {
            "type": "json",
          },
          "inputSource": Object {
            "type": "http",
            "uris": Array [
              "https://example.com/data.json.gz",
            ],
          },
          "signature": Array [
            "\\"timestamp\\" VARCHAR",
            "\\"agent_category\\" VARCHAR",
            "\\"agent_type\\" VARCHAR",
            "\\"browser\\" VARCHAR",
            "\\"browser_version\\" VARCHAR",
            "\\"city\\" VARCHAR",
            "\\"continent\\" VARCHAR",
            "\\"country\\" VARCHAR",
            "\\"version\\" VARCHAR",
            "\\"event_type\\" VARCHAR",
            "\\"event_subtype\\" VARCHAR",
            "\\"loaded_image\\" VARCHAR",
            "\\"adblock_list\\" VARCHAR",
            "\\"forwarded_for\\" VARCHAR",
            "\\"language\\" VARCHAR",
            "\\"number\\" BIGINT",
            "\\"os\\" VARCHAR",
            "\\"path\\" VARCHAR",
            "\\"platform\\" VARCHAR",
            "\\"referrer\\" VARCHAR",
            "\\"referrer_host\\" VARCHAR",
            "\\"region\\" VARCHAR",
            "\\"remote_address\\" VARCHAR",
            "\\"screen\\" VARCHAR",
            "\\"session\\" VARCHAR",
            "\\"session_length\\" BIGINT",
            "\\"timezone\\" VARCHAR",
            "\\"timezone_offset\\" BIGINT",
            "\\"window\\" VARCHAR",
          ],
        },
        "mainExternalName": "ext",
        "metrics": undefined,
        "mode": "insert",
        "overwriteWhere": undefined,
        "partitionedBy": "hour",
      }
    `);

    const query2 = ingestQueryPatternToQuery(insertQueryPattern);

    expect(query2.toString()).toEqual(query.toString());
  });

  it('works with group by', () => {
    const query = SqlQuery.parse(sane`
      REPLACE INTO "inline_data" OVERWRITE ALL
      WITH "ext" AS (
        SELECT *
        FROM TABLE(
          EXTERN(
            '{"type":"inline","data":"{\\"name\\":\\"Moon\\",\\"num\\":11,\\"strings\\":[\\"A\\", \\"B\\"],\\"ints\\":[1,2],\\"floats\\":[1.1,2.2],\\"msg\\":{\\"hello\\":\\"world\\",\\"letters\\":[{\\"letter\\":\\"A\\",\\"index\\":1},{\\"letter\\":\\"B\\",\\"index\\":2}]}}\\n{\\"name\\":\\"Beam\\",\\"num\\":12,\\"strings\\":[\\"A\\", \\"C\\"],\\"ints\\":[3,4],\\"floats\\":[3.3,4,4],\\"msg\\":{\\"where\\":\\"go\\",\\"for\\":\\"food\\",\\"letters\\":[{\\"letter\\":\\"C\\",\\"index\\":3},{\\"letter\\":\\"D\\",\\"index\\":4}]}}\\n"}',
            '{"type":"json"}'
          )
        ) EXTEND ("name" VARCHAR, "num" BIGINT, "strings" VARCHAR ARRAY, "ints" BIGINT ARRAY, "floats" DOUBLE ARRAY, "msg" TYPE('COMPLEX<json>'))
      )
      SELECT
        "name",
        "num",
        ARRAY_TO_MV("strings") AS "strings",
        "ints",
        "floats",
        COUNT(*) AS "count"
      FROM "ext"
      GROUP BY 1, 2, "strings", 4, 5
      PARTITIONED BY ALL
    `);

    const insertQueryPattern = fitIngestQueryPattern(query);

    expect(insertQueryPattern).toMatchInlineSnapshot(`
      Object {
        "clusteredBy": Array [],
        "destinationTableName": "inline_data",
        "dimensions": Array [
          "\\"name\\"",
          "\\"num\\"",
          "ARRAY_TO_MV(\\"strings\\") AS \\"strings\\"",
          "\\"ints\\"",
          "\\"floats\\"",
        ],
        "filters": Array [],
        "mainExternalConfig": Object {
          "inputFormat": Object {
            "type": "json",
          },
          "inputSource": Object {
            "data": "{\\"name\\":\\"Moon\\",\\"num\\":11,\\"strings\\":[\\"A\\", \\"B\\"],\\"ints\\":[1,2],\\"floats\\":[1.1,2.2],\\"msg\\":{\\"hello\\":\\"world\\",\\"letters\\":[{\\"letter\\":\\"A\\",\\"index\\":1},{\\"letter\\":\\"B\\",\\"index\\":2}]}}
      {\\"name\\":\\"Beam\\",\\"num\\":12,\\"strings\\":[\\"A\\", \\"C\\"],\\"ints\\":[3,4],\\"floats\\":[3.3,4,4],\\"msg\\":{\\"where\\":\\"go\\",\\"for\\":\\"food\\",\\"letters\\":[{\\"letter\\":\\"C\\",\\"index\\":3},{\\"letter\\":\\"D\\",\\"index\\":4}]}}
      ",
            "type": "inline",
          },
          "signature": Array [
            "\\"name\\" VARCHAR",
            "\\"num\\" BIGINT",
            "\\"strings\\" VARCHAR ARRAY",
            "\\"ints\\" BIGINT ARRAY",
            "\\"floats\\" DOUBLE ARRAY",
            "\\"msg\\" TYPE('COMPLEX<json>')",
          ],
        },
        "mainExternalName": "ext",
        "metrics": Array [
          "COUNT(*) AS \\"count\\"",
        ],
        "mode": "replace",
        "overwriteWhere": undefined,
        "partitionedBy": "all",
      }
    `);

    const query2 = ingestQueryPatternToQuery(insertQueryPattern);

    expect(query2.toString()).toEqual(query.toString());
  });
});
