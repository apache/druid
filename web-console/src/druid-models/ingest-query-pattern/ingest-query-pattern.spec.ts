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
  it('works', () => {
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
    expect(insertQueryPattern.partitionedBy).toEqual('hour');

    const query2 = ingestQueryPatternToQuery(insertQueryPattern);

    expect(query2.toString()).toEqual(query.toString());
  });
});
