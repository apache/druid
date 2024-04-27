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

import { WorkbenchQuery } from './workbench-query';

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
            '{"type":"http","uris":["https://static.imply.io/example-data/kttm-v2/kttm-v2-2019-08-25.json.gz"]}',
            '{"type":"json"}'
          )
        ) EXTEND ("timestamp" VARCHAR, "agent_type" VARCHAR)
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
      ===== Query =====

      SELECT * FROM q

      ===== Context =====

      {
        "maxNumTasks": 3
      }
    `;

    expect(String(WorkbenchQuery.fromString(tabString))).toEqual(tabString);
  });

  describe('.getRowColumnFromIssue', () => {
    it('works when it can not find at line', () => {
      expect(WorkbenchQuery.getRowColumnFromIssue(`lol`)).toBeUndefined();
    });

    it('works when it can find at line', () => {
      expect(
        WorkbenchQuery.getRowColumnFromIssue(
          `End of input while parsing an object (missing '}') at line 40,2 >>>} ...`,
        ),
      ).toEqual({
        row: 39,
        column: 1,
      });
    });
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
            '{"type":"json"}'
          )
        ) EXTEND ("timestamp" VARCHAR, "agent_type" VARCHAR)
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
            '{"type":"json"}'
          )
        ) EXTEND ("timestamp" VARCHAR, "agent_type" VARCHAR)
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
        prefixLines: 0,
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
        prefixLines: 0,
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
            sqlStringifyArrays: false,
            useCache: false,
          },
          header: true,
          query: 'SELECT * FROM wikipedia',
          resultFormat: 'array',
          sqlTypesHeader: true,
          typesHeader: true,
        },
        prefixLines: 0,
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
            sqlStringifyArrays: false,
          },
          header: true,
          query: 'SELECT * FROM wikipedia',
          resultFormat: 'array',
          sqlTypesHeader: true,
          typesHeader: true,
        },
        prefixLines: 0,
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
            sqlStringifyArrays: false,
            useCache: false,
            x: 1,
          },
          header: true,
          query: 'SELECT * FROM wikipedia',
          resultFormat: 'array',
          sqlTypesHeader: true,
          typesHeader: true,
        },
        prefixLines: 0,
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
            sqlStringifyArrays: false,
            x: 1,
          },
          header: true,
          query: 'SELECT * FROM wikipedia',
          resultFormat: 'array',
          sqlTypesHeader: true,
          typesHeader: true,
        },
        prefixLines: 0,
      });
    });

    it('works with sql-task (as SQL string)', () => {
      const sql = `INSERT INTO wiki2 SELECT * FROM wikipedia`;

      const workbenchQuery = WorkbenchQuery.blank()
        .changeQueryString(sql)
        .changeQueryContext({ useCache: false });

      const apiQuery = workbenchQuery.getApiQuery(makeQueryId);
      expect(apiQuery).toEqual({
        cancelQueryId: undefined,
        engine: 'sql-msq-task',
        query: {
          context: {
            executionMode: 'async',
            finalizeAggregations: false,
            groupByEnableMultiValueUnnesting: false,
            sqlStringifyArrays: false,
            useCache: false,
            waitUntilSegmentsLoad: true,
          },
          header: true,
          query: 'INSERT INTO wiki2 SELECT * FROM wikipedia',
          resultFormat: 'array',
          sqlTypesHeader: true,
          typesHeader: true,
        },
        prefixLines: 0,
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

  describe('#getIssue', () => {
    it('works', () => {
      expect(
        WorkbenchQuery.blank()
          .changeQueryString(
            sane`
              {
                lol: 1
            `,
          )
          .getIssue(),
      ).toEqual("End of input while parsing an object (missing '}') at line 2,9 >>>  lol: 1 ...");
    });
  });
});
