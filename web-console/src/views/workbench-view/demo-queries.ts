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

import { TabEntry, WorkbenchQuery } from '../../druid-models';

const BASE_QUERY = WorkbenchQuery.blank();

export function getDemoQueries(): TabEntry[] {
  function makeDemoQuery(queryString: string): WorkbenchQuery {
    return BASE_QUERY.duplicate().changeQueryString(queryString.trim());
  }

  return [
    {
      id: 'demo1',
      tabName: 'Demo 1',
      query: makeDemoQuery(
        `
-- Demo 1 showcases the new SQL syntax available as part of the sql-msq-task engine.
--
-- The two syntax pieces highlighted in this demo are:
--
-- 1. The REPLACE clause wraps a query and specifies that the result of the underlying query should be written to a datasource.
--    You can also use INSERT instead of REPLACE to append data instead of replacing it.
--
-- 2. The TABLE(EXTERN(...)) function defines a external table that can be used anywhere a table can be used.
--
-- This demo reads the file located at https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz and writes it into
-- the "kttm_simple" datasource (overwriting it if it already exists).
--
-- You can click the "Preview" button if you want to see a preview of the shape of the data that will be ingested and then
-- click "Run" to actually create the kttm_simple datasource (it will be needed for later demos).
--
-- The ingestion here is as simple as it could be (SELECT *), in "Demo 2" you will perform more transformations on the data.

REPLACE INTO "kttm_simple" OVERWRITE ALL
SELECT *
FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz"]}',
    '{"type":"json"}',
    '[{"name":"timestamp","type":"string"},{"name":"agent_category","type":"string"},{"name":"agent_type","type":"string"},{"name":"browser","type":"string"},{"name":"browser_version","type":"string"},{"name":"city","type":"string"},{"name":"continent","type":"string"},{"name":"country","type":"string"},{"name":"version","type":"string"},{"name":"event_type","type":"string"},{"name":"event_subtype","type":"string"},{"name":"loaded_image","type":"string"},{"name":"adblock_list","type":"string"},{"name":"forwarded_for","type":"string"},{"name":"language","type":"string"},{"name":"number","type":"long"},{"name":"os","type":"string"},{"name":"path","type":"string"},{"name":"platform","type":"string"},{"name":"referrer","type":"string"},{"name":"referrer_host","type":"string"},{"name":"region","type":"string"},{"name":"remote_address","type":"string"},{"name":"screen","type":"string"},{"name":"session","type":"string"},{"name":"session_length","type":"long"},{"name":"timezone","type":"string"},{"name":"timezone_offset","type":"long"},{"name":"window","type":"string"}]'
  )
)
PARTITIONED BY ALL TIME
`,
      ),
    },
    {
      id: 'demo2',
      tabName: 'Demo 2',
      query: makeDemoQuery(
        `
-- In Demo 2, you build on the concepts of Demo 1 by transforming data.
-- You're no longer simply reading data from a file and writing it to a datasource.
--
-- In this ingestion, you:
--  - select only a handful of columns.
--  - parse the "timestamp" column, truncate it to MINUTE, and make it the "primary time column" by aliasing it to __time.
--  - filter the rows with a WHERE clause so that only data for 'iOS' is ingested.
--  - perform "rollup" by applying a GROUP BY clause and specifying some aggregates ("metrics").
--  - apply time based partitioning as well as clustering on two columns.
--
-- In Demo 3, you'll enrich data at ingestion time by transforming it further.

REPLACE INTO "kttm_rollup" OVERWRITE ALL

WITH kttm_data AS (
SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz"]}',
    '{"type":"json"}',
    '[{"name":"timestamp","type":"string"},{"name":"agent_category","type":"string"},{"name":"agent_type","type":"string"},{"name":"browser","type":"string"},{"name":"browser_version","type":"string"},{"name":"city","type":"string"},{"name":"continent","type":"string"},{"name":"country","type":"string"},{"name":"version","type":"string"},{"name":"event_type","type":"string"},{"name":"event_subtype","type":"string"},{"name":"loaded_image","type":"string"},{"name":"adblock_list","type":"string"},{"name":"forwarded_for","type":"string"},{"name":"language","type":"string"},{"name":"number","type":"long"},{"name":"os","type":"string"},{"name":"path","type":"string"},{"name":"platform","type":"string"},{"name":"referrer","type":"string"},{"name":"referrer_host","type":"string"},{"name":"region","type":"string"},{"name":"remote_address","type":"string"},{"name":"screen","type":"string"},{"name":"session","type":"string"},{"name":"session_length","type":"long"},{"name":"timezone","type":"string"},{"name":"timezone_offset","type":"long"},{"name":"window","type":"string"}]'
  )
))

SELECT
  FLOOR(TIME_PARSE("timestamp") TO MINUTE) AS __time,
  session,
  agent_category,
  agent_type,
  browser,
  browser_version,
  MV_TO_ARRAY("language") AS "language",
  os,
  city,
  country,
  forwarded_for AS ip_address,

  COUNT(*) AS "cnt",
  SUM(session_length) AS session_length,
  APPROX_COUNT_DISTINCT_DS_HLL(event_type) AS unique_event_types
FROM kttm_data
WHERE os = 'iOS'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
PARTITIONED BY HOUR
CLUSTERED BY browser, session
`,
      ),
    },
    {
      id: 'demo3',
      tabName: 'Demo 3',
      query: makeDemoQuery(
        `
-- This demo has the same query as Demo 2, except that it utilizes an additional external data file (you can have as many
-- as you want) to enrich the data via a JOIN. Specifically for every country in the fact data you add a column for Capital
-- and that countries ISO3 code.
-- This query also computes a derived column, "browser_major", by applying a SQL transformation.
--
-- In the next demo you will see how you are not limited to reading external data in these queries.

REPLACE INTO "kttm_transformed" OVERWRITE ALL
WITH
kttm_data AS (
SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz"]}',
    '{"type":"json"}',
    '[{"name":"timestamp","type":"string"},{"name":"agent_category","type":"string"},{"name":"agent_type","type":"string"},{"name":"browser","type":"string"},{"name":"browser_version","type":"string"},{"name":"city","type":"string"},{"name":"continent","type":"string"},{"name":"country","type":"string"},{"name":"version","type":"string"},{"name":"event_type","type":"string"},{"name":"event_subtype","type":"string"},{"name":"loaded_image","type":"string"},{"name":"adblock_list","type":"string"},{"name":"forwarded_for","type":"string"},{"name":"language","type":"string"},{"name":"number","type":"long"},{"name":"os","type":"string"},{"name":"path","type":"string"},{"name":"platform","type":"string"},{"name":"referrer","type":"string"},{"name":"referrer_host","type":"string"},{"name":"region","type":"string"},{"name":"remote_address","type":"string"},{"name":"screen","type":"string"},{"name":"session","type":"string"},{"name":"session_length","type":"long"},{"name":"timezone","type":"string"},{"name":"timezone_offset","type":"long"},{"name":"window","type":"string"}]'
  )
)),
country_lookup AS (
SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/example-data/lookup/countries.tsv"]}',
    '{"type":"tsv","findColumnsFromHeader":true}',
    '[{"name":"Country","type":"string"},{"name":"Capital","type":"string"},{"name":"ISO3","type":"string"},{"name":"ISO2","type":"string"}]'
  )
))

SELECT
  FLOOR(TIME_PARSE("timestamp") TO MINUTE) AS __time,
  session,
  agent_category,
  agent_type,
  browser,
  browser_version,
  CAST(REGEXP_EXTRACT(browser_version, '^(\\d+)') AS BIGINT) AS browser_major,
  MV_TO_ARRAY("language") AS "language",
  os,
  city,
  country,
  country_lookup.Capital AS capital,
  country_lookup.ISO3 AS iso3,
  forwarded_for AS ip_address,

  COUNT(*) AS "cnt",
  SUM(session_length) AS session_length,
  APPROX_COUNT_DISTINCT_DS_HLL(event_type) AS unique_event_types
FROM kttm_data
LEFT JOIN country_lookup ON country_lookup.Country = kttm_data.country
WHERE os = 'iOS'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
PARTITIONED BY HOUR
CLUSTERED BY browser, session
`,
      ),
    },
    {
      id: 'demo4a',
      tabName: 'Demo 4a',
      query: makeDemoQuery(
        `
-- This demo has a query that is identical to the previous demo except instead of reading the fact data from an external
-- file, it reads the "kttm_simple" datasource that was created in Demo 1.
-- This shows you that you can mix and match data already stored in Druid with external data and transform as needed.
--
-- In the next demo you will look at another type of data transformation.

REPLACE INTO "kttm_reingest" OVERWRITE ALL
WITH
country_lookup AS (
SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/example-data/lookup/countries.tsv"]}',
    '{"type":"tsv","findColumnsFromHeader":true}',
    '[{"name":"Country","type":"string"},{"name":"Capital","type":"string"},{"name":"ISO3","type":"string"},{"name":"ISO2","type":"string"}]'
  )
))

SELECT
  FLOOR(TIME_PARSE("timestamp") TO MINUTE) AS __time,
  session,
  agent_category,
  agent_type,
  browser,
  browser_version,
  CAST(REGEXP_EXTRACT(browser_version, '^(\\d+)') AS BIGINT) AS browser_major,
  MV_TO_ARRAY("language") AS "language",
  os,
  city,
  country,
  country_lookup.Capital AS capital,
  country_lookup.ISO3 AS iso3,
  forwarded_for AS ip_address,

  COUNT(*) AS "cnt",
  SUM(session_length) AS session_length,
  APPROX_COUNT_DISTINCT_DS_HLL(event_type) AS unique_event_types
FROM kttm_simple
LEFT JOIN country_lookup ON country_lookup.Country = kttm_simple.country
WHERE os = 'iOS'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
PARTITIONED BY HOUR
CLUSTERED BY browser, session
`,
      ),
    },
    {
      id: 'demo4b',
      tabName: 'Demo 4b',
      query: makeDemoQuery(
        `
-- Imagine you are an avid reader of https://www.reddit.com/r/MapsWithoutNZ and want your data to reflect your cartography
-- humor. In this demo you transform the datasource created in Demo 1 to remove all entries for 'New Zealand'.
-- Notice that you can read and write to the same datasource.
-- You can open a new tab and run 'SELECT COUNT(*) FROM kttm_simple' before and after running this query to see the change.

REPLACE INTO kttm_simple OVERWRITE ALL
SELECT *
FROM kttm_simple
WHERE NOT(country = 'New Zealand')
PARTITIONED BY ALL TIME
`,
      ),
    },
    {
      id: 'demo5',
      tabName: 'Demo 5',
      query: makeDemoQuery(
        `
-- You don't have to wrap your query in a REPLACE / INSERT clause to run it with the sql-msq-task engine. In fact, when you
-- use the "Preview" button for any of the earlier demos, it works by removing the REPLACE / INSERT clause and
-- running the query "inline" as a SELECT with a limit.
--
-- If you want to run a one time calculation on data, you can do it without ingesting it into Druid. Doing this
-- takes the same order of magnitude of resources and time as it would to ingest the data into Druid though.
--
-- This functionality should be considered as a tech preview of what the sql-msq-task engine can do and not something for
-- production use (outside of specific, narrow uses like the "Preview" feature in this console).

WITH
kttm_data AS (
SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz"]}',
    '{"type":"json"}',
    '[{"name":"timestamp","type":"string"},{"name":"agent_category","type":"string"},{"name":"agent_type","type":"string"},{"name":"browser","type":"string"},{"name":"browser_version","type":"string"},{"name":"city","type":"string"},{"name":"continent","type":"string"},{"name":"country","type":"string"},{"name":"version","type":"string"},{"name":"event_type","type":"string"},{"name":"event_subtype","type":"string"},{"name":"loaded_image","type":"string"},{"name":"adblock_list","type":"string"},{"name":"forwarded_for","type":"string"},{"name":"language","type":"string"},{"name":"number","type":"long"},{"name":"os","type":"string"},{"name":"path","type":"string"},{"name":"platform","type":"string"},{"name":"referrer","type":"string"},{"name":"referrer_host","type":"string"},{"name":"region","type":"string"},{"name":"remote_address","type":"string"},{"name":"screen","type":"string"},{"name":"session","type":"string"},{"name":"session_length","type":"long"},{"name":"timezone","type":"string"},{"name":"timezone_offset","type":"long"},{"name":"window","type":"string"}]'
  )
)),
country_lookup AS (
SELECT * FROM TABLE(
  EXTERN(
    '{"type":"http","uris":["https://static.imply.io/example-data/lookup/countries.tsv"]}',
    '{"type":"tsv","findColumnsFromHeader":true}',
    '[{"name":"Country","type":"string"},{"name":"Capital","type":"string"},{"name":"ISO3","type":"string"},{"name":"ISO2","type":"string"}]'
  )
))
SELECT
  os,
  CONCAT(country, ' (', country_lookup.ISO3, ')') AS "country",
  COUNT(DISTINCT session) AS "unique_sessions"
FROM kttm_data
LEFT JOIN country_lookup ON country_lookup.Country = kttm_data.country
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 10
`,
      ),
    },
    {
      id: 'demo6',
      tabName: 'Demo 6',
      query: makeDemoQuery(
        `
-- At the heart of the sql-msq-task engine is the ability to sort arbitrarily large amounts of data. Demo 6
-- re-sorts the kttm_simple datasource on an arbitrary sort condition. This isn't possible with the "sql" or "native"
-- engines since it requires the same order of magnitude of resources as ingesting the entire dataset over again.

SELECT
  session,
  number,
  browser,
  browser_version,
  "language",
  event_type,
  event_subtype
FROM "kttm_simple"
ORDER BY
  session ASC,
  number ASC
`,
      ).changeEngine('sql-msq-task'),
    },
  ];
}
