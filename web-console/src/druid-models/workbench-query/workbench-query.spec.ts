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

  describe('#getEffectiveEngine', () => {
    beforeEach(() => {
      // Reset to default engines before each test
      WorkbenchQuery.setQueryEngines(['native', 'sql-native', 'sql-msq-task']);
    });

    describe('when engine is explicitly set', () => {
      it('returns the explicitly set engine', () => {
        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString('SELECT * FROM wikipedia')
          .changeEngine('sql-msq-task');

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-msq-task');
      });

      it('returns explicit engine even if query suggests different engine', () => {
        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString('INSERT INTO wiki SELECT * FROM wikipedia')
          .changeEngine('sql-native');

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-native');
      });

      it('returns explicit engine for JSON queries', () => {
        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString('{"queryType": "topN", "dataSource": "test"}')
          .changeEngine('sql-native');

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-native');
      });

      it('returns explicit engine even when context has engine set', () => {
        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString('SELECT * FROM wikipedia')
          .changeQueryContext({ engine: 'native' })
          .changeEngine('sql-msq-task');

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-msq-task');
      });
    });

    describe('when context engine is set', () => {
      it('returns sql-native when context engine is native', () => {
        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString('SELECT * FROM wikipedia')
          .changeQueryContext({ engine: 'native' });

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-native');
      });

      it('returns sql-msq-dart when context engine is msq-dart', () => {
        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString('SELECT * FROM wikipedia')
          .changeQueryContext({ engine: 'msq-dart' });

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-msq-dart');
      });

      it('returns sql-native when context engine is native via SET statement', () => {
        const queryWithSet = sane`
          SET engine = 'native';
          SELECT * FROM wikipedia
        `;

        const workbenchQuery = WorkbenchQuery.blank().changeQueryString(queryWithSet);

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-native');
      });

      it('returns sql-msq-dart when context engine is msq-dart via SET statement', () => {
        const queryWithSet = sane`
          SET engine = 'msq-dart';
          SELECT * FROM wikipedia
        `;

        const workbenchQuery = WorkbenchQuery.blank().changeQueryString(queryWithSet);

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-msq-dart');
      });

      it('returns sql-native when context engine is native via JSON context', () => {
        const sqlInJson = sane`
          {
            "query": "SELECT * FROM wikipedia",
            "context": {
              "engine": "native"
            }
          }
        `;

        const workbenchQuery = WorkbenchQuery.blank().changeQueryString(sqlInJson);

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-native');
      });

      it('returns sql-msq-dart when context engine is msq-dart via JSON context', () => {
        const sqlInJson = sane`
          {
            "query": "SELECT * FROM wikipedia",
            "context": {
              "engine": "msq-dart"
            }
          }
        `;

        const workbenchQuery = WorkbenchQuery.blank().changeQueryString(sqlInJson);

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-msq-dart');
      });

      it('prioritizes SET statement engine over JSON context engine', () => {
        const sqlInJson = sane`
          {
            "query": "SET engine = 'msq-dart'; SELECT * FROM wikipedia",
            "context": {
              "engine": "native"
            }
          }
        `;

        const workbenchQuery = WorkbenchQuery.blank().changeQueryString(sqlInJson);

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-msq-dart');
      });

      it('falls through to other logic when context engine is not native or msq-dart', () => {
        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString('SELECT * FROM wikipedia')
          .changeQueryContext({ engine: 'msq-task' });

        // Should fall through to normal logic and return sql-native
        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-native');
      });

      it('falls through to other logic when context engine is not set', () => {
        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString('SELECT * FROM wikipedia')
          .changeQueryContext({ maxNumTasks: 3 });

        // Should fall through to normal logic and return sql-native
        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-native');
      });

      it('handles INSERT query with context engine native', () => {
        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString('INSERT INTO wiki SELECT * FROM wikipedia')
          .changeQueryContext({ engine: 'native' });

        // Context engine takes priority over task engine detection
        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-native');
      });

      it('handles JSON query with context engine msq-dart', () => {
        const nativeJson = sane`
          {
            "queryType": "topN",
            "dataSource": "wikipedia",
            "context": {
              "engine": "msq-dart"
            }
          }
        `;

        const workbenchQuery = WorkbenchQuery.blank().changeQueryString(nativeJson);

        // Context engine takes priority over JSON-like detection
        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-msq-dart');
      });
    });

    describe('when query is JSON-like', () => {
      it('returns sql-native for SQL-in-JSON when sql-native is enabled', () => {
        const sqlInJson = sane`
          {
            "query": "SELECT * FROM wikipedia",
            "context": {}
          }
        `;

        const workbenchQuery = WorkbenchQuery.blank().changeQueryString(sqlInJson);

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-native');
      });

      it('returns native for native JSON query when native is enabled', () => {
        const nativeJson = sane`
          {
            "queryType": "topN",
            "dataSource": "wikipedia",
            "dimension": "page",
            "threshold": 10,
            "intervals": ["2015-09-12/2015-09-13"],
            "granularity": "all",
            "aggregations": [
              {"type": "count", "name": "count"}
            ]
          }
        `;

        const workbenchQuery = WorkbenchQuery.blank().changeQueryString(nativeJson);

        expect(workbenchQuery.getEffectiveEngine()).toBe('native');
      });

      it('falls through for SQL-in-JSON when sql-native is not enabled', () => {
        WorkbenchQuery.setQueryEngines(['native', 'sql-msq-task']);

        const sqlInJson = sane`
          {
            "query": "SELECT * FROM wikipedia",
            "context": {}
          }
        `;

        const workbenchQuery = WorkbenchQuery.blank().changeQueryString(sqlInJson);

        // Falls through JSON-like check, task engine check (no INSERT/EXTERN), sql-native check (not enabled),
        // and returns first enabled engine which is 'native'
        expect(workbenchQuery.getEffectiveEngine()).toBe('native');
      });

      it('falls through for native JSON when native is not enabled', () => {
        WorkbenchQuery.setQueryEngines(['sql-native', 'sql-msq-task']);

        const nativeJson = sane`
          {
            "queryType": "topN",
            "dataSource": "wikipedia"
          }
        `;

        const workbenchQuery = WorkbenchQuery.blank().changeQueryString(nativeJson);

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-native');
      });
    });

    describe('when query needs task engine', () => {
      it('returns sql-msq-task for INSERT query when sql-msq-task is enabled', () => {
        const insertQuery = 'INSERT INTO wiki SELECT * FROM wikipedia';

        const workbenchQuery = WorkbenchQuery.blank().changeQueryString(insertQuery);

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-msq-task');
      });

      it('returns sql-msq-task for REPLACE query when sql-msq-task is enabled', () => {
        const replaceQuery = 'REPLACE INTO wiki OVERWRITE ALL SELECT * FROM wikipedia';

        const workbenchQuery = WorkbenchQuery.blank().changeQueryString(replaceQuery);

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-msq-task');
      });

      it('returns sql-msq-task for EXTERN query when sql-msq-task is enabled', () => {
        const externQuery = sane`
          SELECT *
          FROM TABLE(
            EXTERN(
              '{"type":"http","uris":["https://example.com/data.json"]}',
              '{"type":"json"}'
            )
          )
        `;

        const workbenchQuery = WorkbenchQuery.blank().changeQueryString(externQuery);

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-msq-task');
      });

      it('falls through when sql-msq-task is not enabled for task engine query', () => {
        WorkbenchQuery.setQueryEngines(['native', 'sql-native']);

        const insertQuery = 'INSERT INTO wiki SELECT * FROM wikipedia';

        const workbenchQuery = WorkbenchQuery.blank().changeQueryString(insertQuery);

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-native');
      });
    });

    describe('fallback behavior', () => {
      it('falls back to sql-native for regular SQL query', () => {
        const regularQuery = 'SELECT * FROM wikipedia';

        const workbenchQuery = WorkbenchQuery.blank().changeQueryString(regularQuery);

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-native');
      });

      it('falls back to sql-native when it is in enabled engines', () => {
        WorkbenchQuery.setQueryEngines(['native', 'sql-msq-task', 'sql-native']);

        const regularQuery = "SELECT * FROM wikipedia WHERE channel = 'en'";

        const workbenchQuery = WorkbenchQuery.blank().changeQueryString(regularQuery);

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-native');
      });

      it('falls back to first enabled engine when sql-native is not available', () => {
        WorkbenchQuery.setQueryEngines(['native', 'sql-msq-task']);

        const regularQuery = 'SELECT * FROM wikipedia';

        const workbenchQuery = WorkbenchQuery.blank().changeQueryString(regularQuery);

        expect(workbenchQuery.getEffectiveEngine()).toBe('native');
      });

      it('falls back to sql-native when no engines are enabled', () => {
        WorkbenchQuery.setQueryEngines([]);

        const regularQuery = 'SELECT * FROM wikipedia';

        const workbenchQuery = WorkbenchQuery.blank().changeQueryString(regularQuery);

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-native');
      });
    });

    describe('complex scenarios', () => {
      it('prioritizes explicit engine over task engine detection', () => {
        const insertQuery = 'INSERT INTO wiki SELECT * FROM wikipedia';

        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString(insertQuery)
          .changeEngine('sql-native');

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-native');
      });

      it('handles SQL query with different enabled engines order', () => {
        WorkbenchQuery.setQueryEngines(['sql-msq-task', 'native', 'sql-native']);

        const regularQuery = 'SELECT COUNT(*) FROM wikipedia';

        const workbenchQuery = WorkbenchQuery.blank().changeQueryString(regularQuery);

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-native');
      });

      it('returns sql-msq-task for task query even when sql-native is enabled', () => {
        WorkbenchQuery.setQueryEngines(['sql-native', 'sql-msq-task', 'native']);

        const insertQuery = sane`
          INSERT INTO wiki
          SELECT * FROM wikipedia
          PARTITIONED BY DAY
        `;

        const workbenchQuery = WorkbenchQuery.blank().changeQueryString(insertQuery);

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-msq-task');
      });

      it('handles empty query string', () => {
        const workbenchQuery = WorkbenchQuery.blank();

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-native');
      });

      it('handles query with only whitespace', () => {
        const workbenchQuery = WorkbenchQuery.blank().changeQueryString('   \n\t  ');

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-native');
      });

      it('handles malformed JSON query', () => {
        const malformedJson = '{ "queryType": "topN"';

        const workbenchQuery = WorkbenchQuery.blank().changeQueryString(malformedJson);

        // Malformed JSON will be treated as JSON-like (starts with {) but will fail isSqlInJson check,
        // falling into the native JSON branch which returns 'native' since it's enabled
        expect(workbenchQuery.getEffectiveEngine()).toBe('native');
      });

      it('correctly identifies case-insensitive INSERT keyword', () => {
        const insertQuery = 'insert into wiki select * from wikipedia';

        const workbenchQuery = WorkbenchQuery.blank().changeQueryString(insertQuery);

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-msq-task');
      });

      it('correctly identifies case-insensitive EXTERN keyword', () => {
        const externQuery = sane`
          SELECT * FROM TABLE(
            extern(
              '{"type":"http","uris":["https://example.com/data.json"]}',
              '{"type":"json"}'
            )
          )
        `;

        const workbenchQuery = WorkbenchQuery.blank().changeQueryString(externQuery);

        expect(workbenchQuery.getEffectiveEngine()).toBe('sql-msq-task');
      });
    });
  });

  describe('#getEffectiveContext', () => {
    describe('for regular SQL queries', () => {
      it('returns queryContext when no SET statements exist', () => {
        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString('SELECT * FROM wikipedia')
          .changeQueryContext({ maxNumTasks: 3, useCache: false });

        const effectiveContext = workbenchQuery.getEffectiveContext();

        expect(effectiveContext).toEqual({
          maxNumTasks: 3,
          useCache: false,
        });
      });

      it('merges queryContext with SET statement context', () => {
        const queryWithSets = sane`
          SET maxNumTasks = 5;
          SET timeout = 30000;
          SELECT * FROM wikipedia
        `;

        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString(queryWithSets)
          .changeQueryContext({ useCache: false, finalizeAggregations: true });

        const effectiveContext = workbenchQuery.getEffectiveContext();

        expect(effectiveContext).toEqual({
          useCache: false,
          finalizeAggregations: true,
          maxNumTasks: 5,
          timeout: 30000,
        });
      });

      it('prioritizes SET statement context over queryContext', () => {
        const queryWithSets = sane`
          SET maxNumTasks = 10;
          SELECT * FROM wikipedia
        `;

        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString(queryWithSets)
          .changeQueryContext({ maxNumTasks: 3, useCache: false });

        const effectiveContext = workbenchQuery.getEffectiveContext();

        expect(effectiveContext.maxNumTasks).toBe(10);
        expect(effectiveContext.useCache).toBe(false);
      });

      it('handles empty query string', () => {
        const workbenchQuery = WorkbenchQuery.blank().changeQueryContext({
          maxNumTasks: 3,
        });

        const effectiveContext = workbenchQuery.getEffectiveContext();

        expect(effectiveContext).toEqual({ maxNumTasks: 3 });
      });

      it('handles query with only SET statements', () => {
        const queryWithOnlySets = sane`
          SET maxNumTasks = 5;
          SET useCache = TRUE;
        `;

        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString(queryWithOnlySets)
          .changeQueryContext({ timeout: 60000 });

        const effectiveContext = workbenchQuery.getEffectiveContext();

        expect(effectiveContext).toEqual({
          timeout: 60000,
          maxNumTasks: 5,
          useCache: true,
        });
      });
    });

    describe('for native JSON queries', () => {
      it('returns queryContext when JSON has no context property', () => {
        const nativeJson = sane`
          {
            "queryType": "topN",
            "dataSource": "wikipedia"
          }
        `;

        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString(nativeJson)
          .changeQueryContext({ maxNumTasks: 3 });

        const effectiveContext = workbenchQuery.getEffectiveContext();

        expect(effectiveContext).toEqual({ maxNumTasks: 3 });
      });

      it('merges JSON context with queryContext', () => {
        const nativeJson = sane`
          {
            "queryType": "topN",
            "dataSource": "wikipedia",
            "context": {
              "timeout": 30000,
              "useCache": false
            }
          }
        `;

        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString(nativeJson)
          .changeQueryContext({ maxNumTasks: 3 });

        const effectiveContext = workbenchQuery.getEffectiveContext();

        expect(effectiveContext).toEqual({
          maxNumTasks: 3,
          timeout: 30000,
          useCache: false,
        });
      });

      it('prioritizes JSON context over queryContext', () => {
        const nativeJson = sane`
          {
            "queryType": "topN",
            "dataSource": "wikipedia",
            "context": {
              "maxNumTasks": 10
            }
          }
        `;

        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString(nativeJson)
          .changeQueryContext({ maxNumTasks: 3, useCache: false });

        const effectiveContext = workbenchQuery.getEffectiveContext();

        expect(effectiveContext.maxNumTasks).toBe(10);
        expect(effectiveContext.useCache).toBe(false);
      });
    });

    describe('for SQL-in-JSON queries', () => {
      it('returns queryContext when JSON has no context and SQL has no SET statements', () => {
        const sqlInJson = sane`
          {
            "query": "SELECT * FROM wikipedia"
          }
        `;

        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString(sqlInJson)
          .changeQueryContext({ maxNumTasks: 3 });

        const effectiveContext = workbenchQuery.getEffectiveContext();

        expect(effectiveContext).toEqual({ maxNumTasks: 3 });
      });

      it('merges JSON context with queryContext', () => {
        const sqlInJson = sane`
          {
            "query": "SELECT * FROM wikipedia",
            "context": {
              "timeout": 30000
            }
          }
        `;

        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString(sqlInJson)
          .changeQueryContext({ maxNumTasks: 3 });

        const effectiveContext = workbenchQuery.getEffectiveContext();

        expect(effectiveContext).toEqual({
          maxNumTasks: 3,
          timeout: 30000,
        });
      });

      it('merges SQL SET statements context with queryContext', () => {
        const sqlInJson = sane`
          {
            "query": "SET useCache = FALSE; SELECT * FROM wikipedia"
          }
        `;

        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString(sqlInJson)
          .changeQueryContext({ maxNumTasks: 3 });

        const effectiveContext = workbenchQuery.getEffectiveContext();

        expect(effectiveContext).toEqual({
          maxNumTasks: 3,
          useCache: false,
        });
      });

      it('merges all three contexts: queryContext, JSON context, and SET statements', () => {
        const sqlInJson = sane`
          {
            "query": "SET timeout = 60000; SET finalizeAggregations = TRUE; SELECT * FROM wikipedia",
            "context": {
              "maxNumTasks": 5,
              "useCache": false
            }
          }
        `;

        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString(sqlInJson)
          .changeQueryContext({ maxNumTasks: 3, priority: 10 });

        const effectiveContext = workbenchQuery.getEffectiveContext();

        expect(effectiveContext).toEqual({
          priority: 10,
          maxNumTasks: 5,
          useCache: false,
          timeout: 60000,
          finalizeAggregations: true,
        });
      });

      it('prioritizes SET statements over JSON context over queryContext', () => {
        const sqlInJson = sane`
          {
            "query": "SET maxNumTasks = 20; SELECT * FROM wikipedia",
            "context": {
              "maxNumTasks": 10
            }
          }
        `;

        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString(sqlInJson)
          .changeQueryContext({ maxNumTasks: 3 });

        const effectiveContext = workbenchQuery.getEffectiveContext();

        expect(effectiveContext.maxNumTasks).toBe(20);
      });

      it('handles SQL-in-JSON with multiple SET statements', () => {
        const sqlInJson = sane`
          {
            "query": "SET maxNumTasks = 8; SET useCache = TRUE; SET timeout = 45000; SELECT * FROM wikipedia",
            "context": {
              "finalizeAggregations": false
            }
          }
        `;

        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString(sqlInJson)
          .changeQueryContext({ priority: 5 });

        const effectiveContext = workbenchQuery.getEffectiveContext();

        expect(effectiveContext).toEqual({
          priority: 5,
          finalizeAggregations: false,
          maxNumTasks: 8,
          useCache: true,
          timeout: 45000,
        });
      });
    });

    describe('error handling', () => {
      it('handles malformed JSON gracefully', () => {
        const malformedJson = '{ "queryType": "topN"';

        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString(malformedJson)
          .changeQueryContext({ maxNumTasks: 3 });

        const effectiveContext = workbenchQuery.getEffectiveContext();

        // Should fall back to queryContext only since JSON parsing fails
        expect(effectiveContext).toEqual({ maxNumTasks: 3 });
      });

      it('handles JSON with invalid context property', () => {
        const jsonWithInvalidContext = sane`
          {
            "queryType": "topN",
            "context": "not an object"
          }
        `;

        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString(jsonWithInvalidContext)
          .changeQueryContext({ maxNumTasks: 3 });

        const effectiveContext = workbenchQuery.getEffectiveContext();

        // Should merge the context even if it's not an object
        expect(effectiveContext).toBeDefined();
      });

      it('handles SQL-in-JSON with malformed SET statements', () => {
        const sqlInJson = sane`
          {
            "query": "SET maxNumTasks INVALID; SELECT * FROM wikipedia"
          }
        `;

        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString(sqlInJson)
          .changeQueryContext({ maxNumTasks: 3 });

        const effectiveContext = workbenchQuery.getEffectiveContext();

        // Should still return queryContext even if SET statement is invalid
        expect(effectiveContext).toBeDefined();
        expect(effectiveContext.maxNumTasks).toBe(3);
      });
    });

    describe('edge cases', () => {
      it('handles empty queryContext', () => {
        const workbenchQuery = WorkbenchQuery.blank().changeQueryString(
          'SELECT * FROM wikipedia',
        );

        const effectiveContext = workbenchQuery.getEffectiveContext();

        expect(effectiveContext).toEqual({});
      });

      it('handles whitespace-only query', () => {
        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString('   \n\t  ')
          .changeQueryContext({ maxNumTasks: 3 });

        const effectiveContext = workbenchQuery.getEffectiveContext();

        expect(effectiveContext).toEqual({ maxNumTasks: 3 });
      });

      it('handles JSON with null context', () => {
        const jsonWithNullContext = sane`
          {
            "queryType": "topN",
            "context": null
          }
        `;

        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString(jsonWithNullContext)
          .changeQueryContext({ maxNumTasks: 3 });

        const effectiveContext = workbenchQuery.getEffectiveContext();

        expect(effectiveContext).toBeDefined();
      });

      it('handles complex nested context values', () => {
        const sqlInJson = sane`
          {
            "query": "SELECT * FROM wikipedia",
            "context": {
              "nestedObject": {
                "key1": "value1",
                "key2": 42
              },
              "arrayValue": [1, 2, 3]
            }
          }
        `;

        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString(sqlInJson)
          .changeQueryContext({ maxNumTasks: 3 });

        const effectiveContext = workbenchQuery.getEffectiveContext();

        expect(effectiveContext).toEqual({
          maxNumTasks: 3,
          nestedObject: {
            key1: 'value1',
            key2: 42,
          },
          arrayValue: [1, 2, 3],
        });
      });

      it('preserves boolean false values in context', () => {
        const queryWithSets = sane`
          SET useCache = FALSE;
          SET finalizeAggregations = FALSE;
          SELECT * FROM wikipedia
        `;

        const workbenchQuery = WorkbenchQuery.blank()
          .changeQueryString(queryWithSets)
          .changeQueryContext({ maxNumTasks: 3 });

        const effectiveContext = workbenchQuery.getEffectiveContext();

        expect(effectiveContext).toEqual({
          maxNumTasks: 3,
          useCache: false,
          finalizeAggregations: false,
        });
      });
    });
  });
});
