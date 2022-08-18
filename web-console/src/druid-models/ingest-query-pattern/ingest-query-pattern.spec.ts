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

import { sane, SqlQuery } from 'druid-query-toolkit';

import { fitIngestQueryPattern, ingestQueryPatternToQuery } from './ingest-query-pattern';

describe('ingest-query-pattern', () => {
  it('works', () => {
    const query = SqlQuery.parse(sane`
      INSERT INTO "kttm-2019"
      WITH ext AS (SELECT *
      FROM TABLE(
        EXTERN(
          '{"type":"http","uris":["https://example.com/data.json.gz"]}',
          '{"type":"json"}',
          '[{"name":"timestamp","type":"string"},{"name":"agent_category","type":"string"},{"name":"agent_type","type":"string"},{"name":"browser","type":"string"},{"name":"browser_version","type":"string"},{"name":"city","type":"string"},{"name":"continent","type":"string"},{"name":"country","type":"string"},{"name":"version","type":"string"},{"name":"event_type","type":"string"},{"name":"event_subtype","type":"string"},{"name":"loaded_image","type":"string"},{"name":"adblock_list","type":"string"},{"name":"forwarded_for","type":"string"},{"name":"language","type":"string"},{"name":"number","type":"long"},{"name":"os","type":"string"},{"name":"path","type":"string"},{"name":"platform","type":"string"},{"name":"referrer","type":"string"},{"name":"referrer_host","type":"string"},{"name":"region","type":"string"},{"name":"remote_address","type":"string"},{"name":"screen","type":"string"},{"name":"session","type":"string"},{"name":"session_length","type":"long"},{"name":"timezone","type":"string"},{"name":"timezone_offset","type":"long"},{"name":"window","type":"string"}]'
        )
      ))
      SELECT
        TIME_PARSE("timestamp") AS __time,
        agent_category,
        agent_type,
        browser,
        browser_version
      FROM ext
      PARTITIONED BY HOUR
      CLUSTERED BY 4
    `);

    const insertQueryPattern = fitIngestQueryPattern(query);
    expect(insertQueryPattern.partitionedBy).toEqual('hour');

    const query2 = ingestQueryPatternToQuery(insertQueryPattern);

    expect(query2.toString()).toEqual(query.toString());
  });
});
