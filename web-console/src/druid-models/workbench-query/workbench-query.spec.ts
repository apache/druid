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

import { sane } from 'druid-query-toolkit';

import { WorkbenchQuery } from './workbench-query';
import { WorkbenchQueryPart } from './workbench-query-part';

describe('WorkbenchQuery', () => {
  beforeAll(() => {
    WorkbenchQuery.setQueryEngines(['native', 'sql-native', 'sql-msq-task']);
  });

  describe('.commentOutInsertInto', () => {
    it('works when INSERT INTO is first', () => {
      const sql = sane`
        INSERT INTO trips2
        SELECT * FROM TABLE(EXTERN(''))
      `;

      expect(WorkbenchQuery.commentOutIngestParts(sql)).toEqual(sane`
        --SERT INTO trips2
        SELECT * FROM TABLE(EXTERN(''))
      `);
    });

    it('works when REPLACE INTO is first', () => {
      const sql = sane`
        REPLACE INTO trips2 OVERWRITE ALL
        SELECT * FROM TABLE(EXTERN(''))
      `;

      expect(WorkbenchQuery.commentOutIngestParts(sql)).toEqual(sane`
        --PLACE INTO trips2 OVERWRITE ALL
        SELECT * FROM TABLE(EXTERN(''))
      `);
    });

    it('works when INSERT INTO is not first', () => {
      const sql = sane`
        -- Some comment
        INSERT INTO trips2
        SELECT * FROM TABLE(EXTERN(''))
      `;

      expect(WorkbenchQuery.commentOutIngestParts(sql)).toEqual(sane`
        -- Some comment
        --SERT INTO trips2
        SELECT * FROM TABLE(EXTERN(''))
      `);
    });

    it('works when INSERT INTO is indented', () => {
      const sql = sane`
        -- Some comment
            INSERT INTO trips2
        SELECT * FROM TABLE(EXTERN(''))
      `;

      expect(WorkbenchQuery.commentOutIngestParts(sql)).toEqual(sane`
        -- Some comment
            --SERT INTO trips2
        SELECT * FROM TABLE(EXTERN(''))
      `);
    });
  });

  describe('.getIngestionLines', () => {
    it('works', () => {
      const queryString = sane`
        REPLACE INTO "kttm_simple"
          OVERWRITE ALL
        SELECT *, VAR
        FROM TABLE(
          EXTERN(
            '{"type":"http","uris":["https://static.imply.io/data/kttm/kttm-v2-2019-08-25.json.gz"]}',
            '{"type":"json"}',
            '[{"name":"timestamp","type":"string"},{"name":"agent_category","type":"string"},{"name":"agent_type","type":"string"},{"name":"browser","type":"string"},{"name":"browser_version","type":"string"},{"name":"city","type":"string"},{"name":"continent","type":"string"},{"name":"country","type":"string"},{"name":"version","type":"string"},{"name":"event_type","type":"string"},{"name":"event_subtype","type":"string"},{"name":"loaded_image","type":"string"},{"name":"adblock_list","type":"string"},{"name":"forwarded_for","type":"string"},{"name":"language","type":"string"},{"name":"number","type":"long"},{"name":"os","type":"string"},{"name":"path","type":"string"},{"name":"platform","type":"string"},{"name":"referrer","type":"string"},{"name":"referrer_host","type":"string"},{"name":"region","type":"string"},{"name":"remote_address","type":"string"},{"name":"screen","type":"string"},{"name":"session","type":"string"},{"name":"session_length","type":"long"},{"name":"timezone","type":"string"},{"name":"timezone_offset","type":"long"},{"name":"window","type":"string"}]'
          )
        )
        PARTITIONED BY ALL TIME
        CLUSTERED BY browser, session
      `;

      expect(WorkbenchQuery.getIngestionLines(queryString)).toEqual({
        clusteredByLine: 'CLUSTERED BY browser, session',
        insertReplaceLine: 'REPLACE INTO "kttm_simple"',
        overwriteLine: '  OVERWRITE ALL',
        partitionedByLine: 'PARTITIONED BY ALL TIME',
      });
    });
  });

  describe('.fromString', () => {
    const tabString = sane`
      ===== Helper: q =====

      SELECT *

      FROM wikipedia

      ===== Query =====

      SELECT * FROM q

      ===== Context =====

      {
        "maxNumTasks": 3
      }
    `;

    expect(String(WorkbenchQuery.fromString(tabString))).toEqual(tabString);
  });

  describe('#makePreview', () => {
    it('works', () => {
      const workbenchQuery = WorkbenchQuery.blank().changeQueryString(sane`
        REPLACE INTO "kttm_simple" OVERWRITE ALL
        SELECT
          TIME_PARSE("timestamp") AS __time,
          session
        FROM TABLE(
          EXTERN(
            '{"type":"http","uris":["https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz"]}',
            '{"type":"json"}',
            '[{"name":"timestamp","type":"string"}]'
          )
        )
        PARTITIONED BY HOUR
        CLUSTERED BY browser, session
      `);

      expect(workbenchQuery.makePreview().getQueryString()).toEqual(sane`
        SELECT
          TIME_PARSE("timestamp") AS __time,
          session
        FROM TABLE(
          EXTERN(
            '{"type":"http","uris":["https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz"]}',
            '{"type":"json"}',
            '[{"name":"timestamp","type":"string"}]'
          )
        )
        ORDER BY FLOOR(__time TO HOUR), browser, session
      `);
    });
  });

  describe('#getApiQuery', () => {
    const makeQueryId = () => 'deadbeef-9fb0-499c-8475-ea461e96a4fd';

    it('works with native', () => {
      const native = sane`
        {
          "queryType": "topN",
          "dataSource": "kttm1"
          "dimension": "browser",
          "threshold": 1001,
          "intervals": "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
          "granularity":"all"
          "aggregations": [
            {
              "type": "count",
              "name": "Count"
            }
          ],
          "context": {
            "x": 1
          }
        }
      `;

      const workbenchQuery = WorkbenchQuery.blank()
        .changeQueryString(native)
        .changeQueryContext({ useCache: false });

      const apiQuery = workbenchQuery.getApiQuery(makeQueryId);
      expect(apiQuery).toEqual({
        cancelQueryId: 'deadbeef-9fb0-499c-8475-ea461e96a4fd',
        engine: 'native',
        query: {
          aggregations: [
            {
              name: 'Count',
              type: 'count',
            },
          ],
          context: {
            queryId: 'deadbeef-9fb0-499c-8475-ea461e96a4fd',
            useCache: false,
            x: 1,
          },
          dataSource: 'kttm1',
          dimension: 'browser',
          granularity: 'all',
          intervals: '-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z',
          queryType: 'topN',
          threshold: 1001,
        },
      });
    });

    it('works with native [explicit queryId]', () => {
      const native = sane`
        {
          "queryType": "topN",
          "dataSource": "kttm1"
          "dimension": "browser",
          "threshold": 1001,
          "intervals": "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
          "granularity":"all"
          "aggregations": [
            {
              "type": "count",
              "name": "Count"
            }
          ],
          "context": {
            "x": 1
          }
        }
      `;

      const workbenchQuery = WorkbenchQuery.blank()
        .changeQueryString(native)
        .changeQueryContext({ queryId: 'lol' });

      const apiQuery = workbenchQuery.getApiQuery(makeQueryId);
      expect(apiQuery).toEqual({
        cancelQueryId: 'lol',
        engine: 'native',
        query: {
          aggregations: [
            {
              name: 'Count',
              type: 'count',
            },
          ],
          context: {
            queryId: 'lol',
            x: 1,
          },
          dataSource: 'kttm1',
          dimension: 'browser',
          granularity: 'all',
          intervals: '-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z',
          queryType: 'topN',
          threshold: 1001,
        },
      });
    });

    it('works with sql (as SQL string)', () => {
      const sql = `SELECT * FROM wikipedia`;

      const workbenchQuery = WorkbenchQuery.blank()
        .changeQueryString(sql)
        .changeQueryContext({ useCache: false });

      const apiQuery = workbenchQuery.getApiQuery(makeQueryId);
      expect(apiQuery).toEqual({
        cancelQueryId: 'deadbeef-9fb0-499c-8475-ea461e96a4fd',
        engine: 'sql-native',
        query: {
          context: {
            sqlOuterLimit: 1001,
            sqlQueryId: 'deadbeef-9fb0-499c-8475-ea461e96a4fd',
            useCache: false,
          },
          header: true,
          query: 'SELECT * FROM wikipedia',
          resultFormat: 'array',
          sqlTypesHeader: true,
          typesHeader: true,
        },
        sqlPrefixLines: 0,
      });
    });

    it('works with sql (as SQL string) [explicit sqlQueryId]', () => {
      const sql = `SELECT * FROM wikipedia`;

      const workbenchQuery = WorkbenchQuery.blank()
        .changeQueryString(sql)
        .changeQueryContext({ sqlQueryId: 'lol' });

      const apiQuery = workbenchQuery.getApiQuery(makeQueryId);
      expect(apiQuery).toEqual({
        cancelQueryId: 'lol',
        engine: 'sql-native',
        query: {
          context: {
            sqlOuterLimit: 1001,
            sqlQueryId: 'lol',
          },
          header: true,
          query: 'SELECT * FROM wikipedia',
          resultFormat: 'array',
          sqlTypesHeader: true,
          typesHeader: true,
        },
        sqlPrefixLines: 0,
      });
    });

    it('works with sql (as JSON)', () => {
      const sqlInJson = sane`
        {
          context: {
            sqlOuterLimit: 1001,
            x: 1
          },
          header: true
          query: 'SELECT * FROM wikipedia',
          resultFormat: 'array',
          sqlTypesHeader: true,
          typesHeader: true,
        }
      `;

      const workbenchQuery = WorkbenchQuery.blank()
        .changeQueryString(sqlInJson)
        .changeQueryContext({ useCache: false });

      const apiQuery = workbenchQuery.getApiQuery(makeQueryId);
      expect(apiQuery).toEqual({
        cancelQueryId: 'deadbeef-9fb0-499c-8475-ea461e96a4fd',
        engine: 'sql-native',
        query: {
          context: {
            sqlOuterLimit: 1001,
            sqlQueryId: 'deadbeef-9fb0-499c-8475-ea461e96a4fd',
            useCache: false,
            x: 1,
          },
          header: true,
          query: 'SELECT * FROM wikipedia',
          resultFormat: 'array',
          sqlTypesHeader: true,
          typesHeader: true,
        },
        sqlPrefixLines: 0,
      });
    });

    it('works with sql (as JSON) [explicit sqlQueryId]', () => {
      const sqlInJson = sane`
        {
          context: {
            sqlOuterLimit: 1001,
            x: 1
          },
          header: true
          query: 'SELECT * FROM wikipedia',
          resultFormat: 'array',
          sqlTypesHeader: true,
          typesHeader: true,
        }
      `;

      const workbenchQuery = WorkbenchQuery.blank()
        .changeQueryString(sqlInJson)
        .changeQueryContext({ sqlQueryId: 'lol' });

      const apiQuery = workbenchQuery.getApiQuery(makeQueryId);
      expect(apiQuery).toEqual({
        cancelQueryId: 'lol',
        engine: 'sql-native',
        query: {
          context: {
            sqlOuterLimit: 1001,
            sqlQueryId: 'lol',
            x: 1,
          },
          header: true,
          query: 'SELECT * FROM wikipedia',
          resultFormat: 'array',
          sqlTypesHeader: true,
          typesHeader: true,
        },
        sqlPrefixLines: 0,
      });
    });

    it('works with sql-task (as SQL string)', () => {
      const sql = `INSERT INTO wiki2 SELECT * FROM wikipedia`;

      const workbenchQuery = WorkbenchQuery.blank()
        .changeQueryString(sql)
        .changeQueryContext({ useCache: false });

      const apiQuery = workbenchQuery.getApiQuery(makeQueryId);
      expect(apiQuery).toEqual({
        engine: 'sql-msq-task',
        query: {
          context: {
            finalizeAggregations: false,
            groupByEnableMultiValueUnnesting: false,
            useCache: false,
          },
          header: true,
          query: 'INSERT INTO wiki2 SELECT * FROM wikipedia',
          resultFormat: 'array',
          sqlTypesHeader: true,
          typesHeader: true,
        },
        sqlPrefixLines: 0,
      });
    });

    it('works with sql with ISSUE comment', () => {
      const sql = sane`
        SELECT *
        --:ISSUE: There is something wrong with this query.
        FROM wikipedia
      `;

      const workbenchQuery = WorkbenchQuery.blank().changeQueryString(sql);

      expect(() => workbenchQuery.getApiQuery(makeQueryId)).toThrow(
        `This query contains an ISSUE comment: There is something wrong with this query. (Please resolve the issue in the comment, delete the ISSUE comment and re-run the query.)`,
      );
    });
  });

  describe('#getIngestDatasource', () => {
    it('works with INSERT', () => {
      const sql = sane`
        -- Some comment
        INSERT INTO trips2
        SELECT
          TIME_PARSE(pickup_datetime) AS __time,
          *
        FROM TABLE(
            EXTERN(
              '{"type": "local", ...}',
              '{"type":"csv", ...}',
              '[{ "name": "cab_type", "type": "string" }, ...]'
            )
          )
        CLUSTERED BY trip_id
      `;

      const workbenchQuery = WorkbenchQuery.blank().changeQueryString(sql);
      expect(workbenchQuery.getIngestDatasource()).toEqual('trips2');
      expect(workbenchQuery.changeEngine('sql-native').getIngestDatasource()).toBeUndefined();
    });

    it('works with INSERT (unparsable)', () => {
      const sql = sane`
        -- Some comment
        INSERT INTO trips2
        SELECT
          TIME_PARSE(pickup_datetime) AS __time,
          *
        FROM TABLE(
      `;

      const workbenchQuery = WorkbenchQuery.blank().changeQueryString(sql);
      expect(workbenchQuery.getIngestDatasource()).toEqual('trips2');
      expect(workbenchQuery.changeEngine('sql-native').getIngestDatasource()).toBeUndefined();
    });

    it('works with REPLACE', () => {
      const sql = sane`
        REPLACE INTO trips2 OVERWRITE ALL
        SELECT
          TIME_PARSE(pickup_datetime) AS __time,
          *
        FROM TABLE(
            EXTERN(
              '{"type": "local", ...}',
              '{"type":"csv", ...}',
              '[{ "name": "cab_type", "type": "string" }, ...]'
            )
          )
        CLUSTERED BY trip_id
      `;

      const workbenchQuery = WorkbenchQuery.blank().changeQueryString(sql);
      expect(workbenchQuery.getIngestDatasource()).toEqual('trips2');
      expect(workbenchQuery.changeEngine('sql-native').getIngestDatasource()).toBeUndefined();
    });

    it('works with REPLACE (unparsable)', () => {
      const sql = sane`
        REPLACE INTO trips2 OVERWRITE ALL
        WITH kttm_data AS (SELECT *
      `;

      const workbenchQuery = WorkbenchQuery.blank().changeQueryString(sql);
      expect(workbenchQuery.getIngestDatasource()).toEqual('trips2');
      expect(workbenchQuery.changeEngine('sql-native').getIngestDatasource()).toBeUndefined();
    });
  });

  describe('#extractCteHelpers', () => {
    it('works', () => {
      const sql = sane`
        REPLACE INTO task_statuses OVERWRITE ALL
        WITH
        task_statuses AS (
        SELECT * FROM
        TABLE(
          EXTERN(
            '{"type":"local","baseDir":"/Users/vadim/Desktop/","filter":"task_statuses.json"}',
            '{"type":"json"}',
            '[{"name":"id","type":"string"},{"name":"status","type":"string"},{"name":"duration","type":"long"},{"name":"errorMsg","type":"string"},{"name":"created_date","type":"string"}]'
          )
        )
        )
        (
        --PLACE INTO task_statuses OVERWRITE ALL
        SELECT
          id,
          status,
          duration,
          errorMsg,
          created_date
        FROM task_statuses
        --RTITIONED BY ALL
        )
        PARTITIONED BY ALL
      `;

      expect(WorkbenchQuery.blank().changeQueryString(sql).extractCteHelpers().getQueryString())
        .toEqual(sane`
          REPLACE INTO task_statuses OVERWRITE ALL
          SELECT
            id,
            status,
            duration,
            errorMsg,
            created_date
          FROM task_statuses
          PARTITIONED BY ALL
        `);
    });
  });

  describe('#materializeHelpers', () => {
    expect(
      WorkbenchQuery.blank()
        .changeQueryParts([
          new WorkbenchQueryPart({
            id: 'aaa',
            queryName: 'kttm_data',
            queryString: sane`
            SELECT * FROM TABLE(
              EXTERN(
                '{"type":"http","uris":["https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz"]}',
                '{"type":"json"}',
                '[{"name":"timestamp","type":"string"},{"name":"agent_category","type":"string"},{"name":"agent_type","type":"string"},{"name":"browser","type":"string"},{"name":"browser_version","type":"string"},{"name":"city","type":"string"},{"name":"continent","type":"string"},{"name":"country","type":"string"},{"name":"version","type":"string"},{"name":"event_type","type":"string"},{"name":"event_subtype","type":"string"},{"name":"loaded_image","type":"string"},{"name":"adblock_list","type":"string"},{"name":"forwarded_for","type":"string"},{"name":"language","type":"string"},{"name":"number","type":"long"},{"name":"os","type":"string"},{"name":"path","type":"string"},{"name":"platform","type":"string"},{"name":"referrer","type":"string"},{"name":"referrer_host","type":"string"},{"name":"region","type":"string"},{"name":"remote_address","type":"string"},{"name":"screen","type":"string"},{"name":"session","type":"string"},{"name":"session_length","type":"long"},{"name":"timezone","type":"string"},{"name":"timezone_offset","type":"long"},{"name":"window","type":"string"}]'
              )
            )
        `,
          }),
          new WorkbenchQueryPart({
            id: 'bbb',
            queryName: 'country_lookup',
            queryString: sane`
            SELECT * FROM TABLE(
              EXTERN(
                '{"type":"http","uris":["https://static.imply.io/example-data/lookup/countries.tsv"]}',
                '{"type":"tsv","findColumnsFromHeader":true}',
                '[{"name":"Country","type":"string"},{"name":"Capital","type":"string"},{"name":"ISO3","type":"string"},{"name":"ISO2","type":"string"}]'
              )
            )
        `,
          }),
          new WorkbenchQueryPart({
            id: 'ccc',
            queryName: 'x',
            queryString: sane`
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
          }),
        ])
        .materializeHelpers(),
    );
  });
});
