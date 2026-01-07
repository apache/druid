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
            engine: 'native',
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
            engine: 'native',
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
            engine: 'native',
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
            engine: 'native',
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

    it('works with sql (preserves explicit engine context)', () => {
      const sql = `SELECT * FROM wikipedia`;

      const workbenchQuery = WorkbenchQuery.blank()
        .changeQueryString(sql)
        .changeQueryContext({ engine: 'msq-task' });

      const apiQuery = workbenchQuery.getApiQuery(makeQueryId);
      expect(apiQuery).toEqual({
        cancelQueryId: 'deadbeef-9fb0-499c-8475-ea461e96a4fd',
        engine: 'sql-native',
        query: {
          context: {
            engine: 'msq-task',
            sqlOuterLimit: 1001,
            sqlQueryId: 'deadbeef-9fb0-499c-8475-ea461e96a4fd',
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

  describe('#changeQueryStringContext', () => {
    it('modifies SQL query string with SET statements', () => {
      const workbenchQuery = WorkbenchQuery.blank().changeQueryString('SELECT * FROM wikipedia');

      const newContext = { maxNumTasks: 3, useCache: false };
      const updatedQuery = workbenchQuery.changeQueryStringContext(newContext);

      expect(updatedQuery.getQueryString()).toContain('SET maxNumTasks = 3');
      expect(updatedQuery.getQueryString()).toContain('SET useCache = FALSE');
      expect(updatedQuery.getQueryString()).toContain('SELECT * FROM wikipedia');
    });

    it('updates existing SET statements in SQL query', () => {
      const workbenchQuery = WorkbenchQuery.blank().changeQueryString(sane`
        SET maxNumTasks = 2;
        SELECT * FROM wikipedia
      `);

      const newContext = { maxNumTasks: 5, finalizeAggregations: true };
      const updatedQuery = workbenchQuery.changeQueryStringContext(newContext);

      expect(updatedQuery.getQueryString()).toContain('SET maxNumTasks = 5');
      expect(updatedQuery.getQueryString()).toContain('SET finalizeAggregations = TRUE');
      expect(updatedQuery.getQueryString()).toContain('SELECT * FROM wikipedia');
    });

    it('works with JSON queries by modifying queryContext instead', () => {
      const jsonQuery = sane`
        {
          "queryType": "topN",
          "dataSource": "test"
        }
      `;

      const workbenchQuery = WorkbenchQuery.blank()
        .changeQueryString(jsonQuery)
        .changeQueryContext({ originalContext: true });

      const newContext = { maxNumTasks: 3, useCache: false };
      const updatedQuery = workbenchQuery.changeQueryStringContext(newContext);

      expect(updatedQuery.queryContext).toEqual(newContext);
      expect(updatedQuery.getQueryString()).toEqual(jsonQuery);
    });

    it('handles empty context object', () => {
      const workbenchQuery = WorkbenchQuery.blank().changeQueryString('SELECT * FROM wikipedia');

      const updatedQuery = workbenchQuery.changeQueryStringContext({});

      expect(updatedQuery.getQueryString()).toBe('SELECT * FROM wikipedia');
    });

    it('preserves original queryContext when working with JSON', () => {
      const jsonQuery = '{"queryType": "timeseries"}';
      const workbenchQuery = WorkbenchQuery.blank()
        .changeQueryString(jsonQuery)
        .changeQueryContext({ existing: 'value' });

      const updatedQuery = workbenchQuery.changeQueryStringContext({ new: 'context' });

      expect(updatedQuery.queryContext).toEqual({ new: 'context' });
    });
  });

  describe('#getQueryStringContext', () => {
    it('extracts context from SQL SET statements', () => {
      const queryWithSets = sane`
        SET maxNumTasks = 3;
        SET useCache = false;
        SET stringParam = 'test';
        SELECT * FROM wikipedia
      `;

      const workbenchQuery = WorkbenchQuery.blank().changeQueryString(queryWithSets);
      const extractedContext = workbenchQuery.getQueryStringContext();

      expect(extractedContext).toEqual({
        maxNumTasks: 3,
        useCache: false,
        stringParam: 'test',
      });
    });

    it('returns empty object when no SET statements in SQL', () => {
      const workbenchQuery = WorkbenchQuery.blank().changeQueryString('SELECT * FROM wikipedia');

      const extractedContext = workbenchQuery.getQueryStringContext();

      expect(extractedContext).toEqual({});
    });

    it('returns queryContext for JSON queries', () => {
      const jsonQuery = '{"queryType": "topN"}';
      const contextValue = { maxNumTasks: 5, useCache: true };

      const workbenchQuery = WorkbenchQuery.blank()
        .changeQueryString(jsonQuery)
        .changeQueryContext(contextValue);

      const extractedContext = workbenchQuery.getQueryStringContext();

      expect(extractedContext).toEqual(contextValue);
    });

    it('handles mixed SET statements and regular SQL', () => {
      const queryWithMixedContent = sane`
        -- Comment
        SET timeout = 30000;
        SET maxRows = 1000;

        SELECT COUNT(*)
        FROM wikipedia
        WHERE channel = 'en'
      `;

      const workbenchQuery = WorkbenchQuery.blank().changeQueryString(queryWithMixedContent);
      const extractedContext = workbenchQuery.getQueryStringContext();

      expect(extractedContext).toEqual({
        timeout: 30000,
        maxRows: 1000,
      });
    });

    it('returns empty object for malformed JSON queries', () => {
      const malformedJson = '{ "queryType": "topN"';
      const workbenchQuery = WorkbenchQuery.blank()
        .changeQueryString(malformedJson)
        .changeQueryContext({ fallback: true });

      const extractedContext = workbenchQuery.getQueryStringContext();

      expect(extractedContext).toEqual({ fallback: true });
    });

    it('handles various data types in SET statements', () => {
      const queryWithVariousTypes = sane`
        SET stringParam = 'text';
        SET numberParam = 42;
        SET booleanParam = TRUE;
        SET nullParam = NULL;
        SELECT * FROM test
      `;

      const workbenchQuery = WorkbenchQuery.blank().changeQueryString(queryWithVariousTypes);
      const extractedContext = workbenchQuery.getQueryStringContext();

      expect(extractedContext).toEqual({
        stringParam: 'text',
        numberParam: 42,
        booleanParam: true,
        nullParam: null,
      });
    });
  });

  describe('changeQueryStringContext and getQueryStringContext symmetry', () => {
    it('maintains symmetry for SQL queries', () => {
      const originalQuery = 'SELECT * FROM wikipedia';
      const testContext = { maxNumTasks: 3, useCache: false, timeout: 30000 };

      const workbenchQuery = WorkbenchQuery.blank().changeQueryString(originalQuery);
      const updatedQuery = workbenchQuery.changeQueryStringContext(testContext);
      const extractedContext = updatedQuery.getQueryStringContext();

      expect(extractedContext).toEqual(testContext);
    });

    it('maintains symmetry for JSON queries', () => {
      const jsonQuery = '{"queryType": "topN", "dataSource": "test"}';
      const testContext = { maxNumTasks: 5, finalizeAggregations: true };

      const workbenchQuery = WorkbenchQuery.blank().changeQueryString(jsonQuery);
      const updatedQuery = workbenchQuery.changeQueryStringContext(testContext);
      const extractedContext = updatedQuery.getQueryStringContext();

      expect(extractedContext).toEqual(testContext);
    });

    it('roundtrip preserves simple context values for SQL queries', () => {
      const simpleContext = {
        stringValue: 'test',
        numberValue: 42,
        booleanValue: true,
        nullValue: null,
      };

      const workbenchQuery = WorkbenchQuery.blank()
        .changeQueryString('SELECT * FROM test')
        .changeQueryStringContext(simpleContext);

      const extractedContext = workbenchQuery.getQueryStringContext();

      expect(extractedContext).toEqual(simpleContext);
    });

    it('roundtrip preserves complex context values for JSON queries', () => {
      const complexContext = {
        stringValue: 'test',
        numberValue: 42,
        booleanValue: true,
        nullValue: null,
        arrayValue: [1, 2, 3],
        objectValue: { nested: { key: 'value' } },
      };

      const workbenchQuery = WorkbenchQuery.blank()
        .changeQueryString('{"queryType": "topN"}')
        .changeQueryStringContext(complexContext);

      const extractedContext = workbenchQuery.getQueryStringContext();

      expect(extractedContext).toEqual(complexContext);
    });
  });

  describe('#getMaxNumTasks', () => {
    it('returns maxNumTasks from query string context for SQL queries with SET statements', () => {
      const queryWithSet = sane`
        SET maxNumTasks = 5;
        SELECT * FROM wikipedia
      `;

      const workbenchQuery = WorkbenchQuery.blank().changeQueryString(queryWithSet);

      expect(workbenchQuery.getMaxNumTasks()).toBe(5);
    });

    it('returns maxNumTasks from queryContext when no SET statements exist', () => {
      const workbenchQuery = WorkbenchQuery.blank()
        .changeQueryString('SELECT * FROM wikipedia')
        .changeQueryContext({ maxNumTasks: 3 });

      expect(workbenchQuery.getMaxNumTasks()).toBe(3);
    });

    it('prioritizes query string context over queryContext', () => {
      const queryWithSet = sane`
        SET maxNumTasks = 8;
        SELECT * FROM wikipedia
      `;

      const workbenchQuery = WorkbenchQuery.blank()
        .changeQueryString(queryWithSet)
        .changeQueryContext({ maxNumTasks: 3 });

      expect(workbenchQuery.getMaxNumTasks()).toBe(8);
    });

    it('returns undefined when maxNumTasks is not set anywhere', () => {
      const workbenchQuery = WorkbenchQuery.blank().changeQueryString('SELECT * FROM wikipedia');

      expect(workbenchQuery.getMaxNumTasks()).toBeUndefined();
    });

    it('returns maxNumTasks from queryContext for JSON queries', () => {
      const jsonQuery = '{"queryType": "topN", "dataSource": "test"}';
      const workbenchQuery = WorkbenchQuery.blank()
        .changeQueryString(jsonQuery)
        .changeQueryContext({ maxNumTasks: 10 });

      expect(workbenchQuery.getMaxNumTasks()).toBe(10);
    });
  });

  describe('#setMaxNumTasksIfUnset', () => {
    it('sets maxNumTasks when it is not already set', () => {
      const workbenchQuery = WorkbenchQuery.blank().changeQueryString('SELECT * FROM wikipedia');

      const updatedQuery = workbenchQuery.setMaxNumTasksIfUnset(5);

      expect(updatedQuery.getMaxNumTasks()).toBe(5);
      expect(updatedQuery.queryContext.maxNumTasks).toBe(5);
    });

    it('does not override existing maxNumTasks in queryContext', () => {
      const workbenchQuery = WorkbenchQuery.blank()
        .changeQueryString('SELECT * FROM wikipedia')
        .changeQueryContext({ maxNumTasks: 3 });

      const updatedQuery = workbenchQuery.setMaxNumTasksIfUnset(10);

      expect(updatedQuery).toBe(workbenchQuery); // Should return same instance
      expect(updatedQuery.getMaxNumTasks()).toBe(3);
    });

    it('does not override maxNumTasks from SET statements', () => {
      const queryWithSet = sane`
        SET maxNumTasks = 7;
        SELECT * FROM wikipedia
      `;

      const workbenchQuery = WorkbenchQuery.blank().changeQueryString(queryWithSet);

      const updatedQuery = workbenchQuery.setMaxNumTasksIfUnset(10);

      expect(updatedQuery).toBe(workbenchQuery); // Should return same instance
      expect(updatedQuery.getMaxNumTasks()).toBe(7);
    });

    it('enforces minimum value of 2', () => {
      const workbenchQuery = WorkbenchQuery.blank().changeQueryString('SELECT * FROM wikipedia');

      const updatedQuery = workbenchQuery.setMaxNumTasksIfUnset(1);

      expect(updatedQuery.getMaxNumTasks()).toBe(2);
    });

    it('returns same instance when passed undefined', () => {
      const workbenchQuery = WorkbenchQuery.blank().changeQueryString('SELECT * FROM wikipedia');

      const updatedQuery = workbenchQuery.setMaxNumTasksIfUnset(undefined);

      expect(updatedQuery).toBe(workbenchQuery);
      expect(updatedQuery.getMaxNumTasks()).toBeUndefined();
    });

    it('returns same instance when passed 0', () => {
      const workbenchQuery = WorkbenchQuery.blank().changeQueryString('SELECT * FROM wikipedia');

      const updatedQuery = workbenchQuery.setMaxNumTasksIfUnset(0);

      expect(updatedQuery).toBe(workbenchQuery);
      expect(updatedQuery.getMaxNumTasks()).toBeUndefined();
    });

    it('preserves other queryContext properties when setting maxNumTasks', () => {
      const workbenchQuery = WorkbenchQuery.blank()
        .changeQueryString('SELECT * FROM wikipedia')
        .changeQueryContext({ useCache: false, timeout: 30000 });

      const updatedQuery = workbenchQuery.setMaxNumTasksIfUnset(5);

      expect(updatedQuery.queryContext).toEqual({
        useCache: false,
        timeout: 30000,
        maxNumTasks: 5,
      });
    });
  });
});
