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

import { QueryResult, sane } from 'druid-query-toolkit';

import { formatForFileFormat, queryResultsToString } from './download';

describe('download', () => {
  it('formatForFormat', () => {
    expect(formatForFileFormat(null, 'csv')).toEqual('');
    expect(formatForFileFormat(null, 'tsv')).toEqual('');
    expect(formatForFileFormat('', 'csv')).toEqual('""');
    expect(formatForFileFormat('null', 'csv')).toEqual('"null"');
    expect(formatForFileFormat('hello\nworld', 'csv')).toEqual('"hello world"');
    expect(formatForFileFormat(123, 'csv')).toEqual('"123"');
    expect(formatForFileFormat(new Date('2021-01-02T03:04:05.678Z'), 'csv')).toEqual(
      '"2021-01-02T03:04:05.678Z"',
    );
  });

  describe('queryResultsToString', () => {
    const queryResult = QueryResult.fromRawResult(
      [
        ['__time', 'browser', 'session_length'],
        ['LONG', 'STRING', 'LONG'],
        ['TIMESTAMP', 'VARCHAR', 'BIGINT'],
        ['1970-01-01T00:00:00.000Z', 'Chrome', 76261],
        ['1970-01-01T00:00:00.000Z', 'Chrome Mobile', 252689],
        ['1970-01-01T00:00:00.000Z', 'Chrome', 1753602],
      ],
      true,
      true,
      true,
      true,
    );

    it('works with CSV', () => {
      expect(queryResultsToString(queryResult, 'csv')).toEqual(sane`
        "__time","browser","session_length"
        "1970-01-01T00:00:00.000Z","Chrome","76261"
        "1970-01-01T00:00:00.000Z","Chrome Mobile","252689"
        "1970-01-01T00:00:00.000Z","Chrome","1753602"
      `);
    });

    it('works with TSV', () => {
      expect(queryResultsToString(queryResult, 'tsv')).toEqual(sane`
        __time	browser	session_length
        1970-01-01T00:00:00.000Z	Chrome	76261
        1970-01-01T00:00:00.000Z	Chrome Mobile	252689
        1970-01-01T00:00:00.000Z	Chrome	1753602
      `);
    });

    it('works with JSON', () => {
      expect(queryResultsToString(queryResult, 'json')).toEqual(sane`
        {"__time":"1970-01-01T00:00:00.000Z","browser":"Chrome","session_length":76261}
        {"__time":"1970-01-01T00:00:00.000Z","browser":"Chrome Mobile","session_length":252689}
        {"__time":"1970-01-01T00:00:00.000Z","browser":"Chrome","session_length":1753602}
      `);
    });

    it('works with SQL', () => {
      expect(queryResultsToString(queryResult, 'sql')).toEqual(sane`
        SELECT
          CAST("c1" AS TIMESTAMP) AS "__time",
          CAST("c2" AS VARCHAR) AS "browser",
          CAST("c3" AS BIGINT) AS "session_length"
        FROM (
          VALUES
          ('1970-01-01T00:00:00.000Z', 'Chrome', 76261),
          ('1970-01-01T00:00:00.000Z', 'Chrome Mobile', 252689),
          ('1970-01-01T00:00:00.000Z', 'Chrome', 1753602)
        ) AS "t" ("c1", "c2", "c3")
      `);
    });

    it('works with Markdown', () => {
      expect(queryResultsToString(queryResult, 'markdown')).toEqual(sane`
        | __time                   | browser       | session_length |
        | :----------------------- | :------------ | -------------: |
        | 1970-01-01T00:00:00.000Z | Chrome        |          76261 |
        | 1970-01-01T00:00:00.000Z | Chrome Mobile |         252689 |
        | 1970-01-01T00:00:00.000Z | Chrome        |        1753602 |
      `);
    });
  });
});
