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

import { DruidError, getDruidErrorMessage } from './druid-query';

describe('DruidQuery', () => {
  describe('DruidError.extractStartRowColumn', () => {
    it('works for single error 1', () => {
      expect(
        DruidError.extractStartRowColumn({
          sourceType: 'sql',
          line: '2',
          column: '12',
          token: "AS \\'l\\'",
          expected: '...',
        }),
      ).toEqual({
        row: 1,
        column: 11,
      });
    });

    it('works for range', () => {
      expect(
        DruidError.extractStartRowColumn({
          sourceType: 'sql',
          line: '1',
          column: '16',
          endLine: '1',
          endColumn: '17',
          token: "AS \\'l\\'",
          expected: '...',
        }),
      ).toEqual({
        row: 0,
        column: 15,
      });
    });
  });

  describe('DruidError.extractEndRowColumn', () => {
    it('works for single error 1', () => {
      expect(
        DruidError.extractEndRowColumn({
          sourceType: 'sql',
          line: '2',
          column: '12',
          token: "AS \\'l\\'",
          expected: '...',
        }),
      ).toBeUndefined();
    });

    it('works for range', () => {
      expect(
        DruidError.extractEndRowColumn({
          sourceType: 'sql',
          line: '1',
          column: '16',
          endLine: '1',
          endColumn: '17',
          token: "AS \\'l\\'",
          expected: '...',
        }),
      ).toEqual({
        row: 0,
        column: 16,
      });
    });
  });

  describe('DruidError.getSuggestion', () => {
    it('works for ==', () => {
      const sql = sane`
        SELECT *
        FROM wikipedia -- test ==
        WHERE channel == '#ar.wikipedia'
      `;
      const suggestion = DruidError.getSuggestion(
        `Received an unexpected token [= =] (line [3], column [15]), acceptable options:`,
      );
      expect(suggestion!.label).toEqual(`Replace == with =`);
      expect(suggestion!.fn(sql)).toEqual(sane`
        SELECT *
        FROM wikipedia -- test ==
        WHERE channel = '#ar.wikipedia'
      `);
    });

    it('works for == 2', () => {
      const sql = sane`
        SELECT
          channel, COUNT(*) AS "Count"
        FROM wikipedia
        WHERE channel == 'de'
        GROUP BY 1
        ORDER BY 2 DESC
      `;
      const suggestion = DruidError.getSuggestion(
        `Received an unexpected token [= =] (line [4], column [15]), acceptable options:`,
      );
      expect(suggestion!.label).toEqual(`Replace == with =`);
      expect(suggestion!.fn(sql)).toEqual(sane`
        SELECT
          channel, COUNT(*) AS "Count"
        FROM wikipedia
        WHERE channel = 'de'
        GROUP BY 1
        ORDER BY 2 DESC
      `);
    });

    it('works for bad double quotes 1', () => {
      const sql = sane`
        SELECT * FROM “wikipedia”
      `;
      const suggestion = DruidError.getSuggestion(
        'Lexical error at line 6, column 60.  Encountered: "\\u201c" (8220), after : ""',
      );
      expect(suggestion!.label).toEqual(`Replace fancy quotes with ASCII quotes`);
      expect(suggestion!.fn(sql)).toEqual(sane`
        SELECT * FROM "wikipedia"
      `);
    });

    it('works for bad double quotes 2', () => {
      const sql = sane`
        SELECT * FROM ”wikipedia”
      `;
      const suggestion = DruidError.getSuggestion(
        'Lexical error at line 6, column 60.  Encountered: "\\u201d" (8221), after : ""',
      );
      expect(suggestion!.label).toEqual(`Replace fancy quotes with ASCII quotes`);
      expect(suggestion!.fn(sql)).toEqual(sane`
        SELECT * FROM "wikipedia"
      `);
    });

    it('works for bad double quotes 3', () => {
      const sql = sane`
        SELECT * FROM "wikipedia" WHERE "channel" = ‘lol‘
      `;
      const suggestion = DruidError.getSuggestion(
        'Lexical error at line 1, column 45. Encountered: "\\u2018" (8216), after : ""',
      );
      expect(suggestion!.label).toEqual(`Replace fancy quotes with ASCII quotes`);
      expect(suggestion!.fn(sql)).toEqual(sane`
        SELECT * FROM "wikipedia" WHERE "channel" = 'lol'
      `);
    });

    it('works for incorrectly quoted literal', () => {
      const sql = sane`
        SELECT *
        FROM wikipedia -- test "#ar.wikipedia"
        WHERE channel = "#ar.wikipedia"
      `;
      const suggestion = DruidError.getSuggestion(
        `Column '#ar.wikipedia' not found in any table (line [3], column [17])`,
      );
      expect(suggestion!.label).toEqual(`Replace "#ar.wikipedia" with '#ar.wikipedia'`);
      expect(suggestion!.fn(sql)).toEqual(sane`
        SELECT *
        FROM wikipedia -- test "#ar.wikipedia"
        WHERE channel = '#ar.wikipedia'
      `);
    });

    it('works for incorrectly quoted AS alias', () => {
      const sql = `SELECT channel AS 'c' FROM wikipedia`;
      const suggestion = DruidError.getSuggestion(
        `Received an unexpected token [AS \\'c\\'] (line [1], column [16]), acceptable options:`,
      );
      expect(suggestion!.label).toEqual(`Replace 'c' with "c"`);
      expect(suggestion!.fn(sql)).toEqual(`SELECT channel AS "c" FROM wikipedia`);
    });

    it('removes comma (,) before FROM', () => {
      const sql = `SELECT page, FROM wikipedia WHERE channel = '#ar.wikipedia'`;
      const suggestion = DruidError.getSuggestion(
        `Received an unexpected token [, FROM] (line [1], column [12]), acceptable options:`,
      );
      expect(suggestion!.label).toEqual(`Remove comma (,) before FROM`);
      expect(suggestion!.fn(sql)).toEqual(
        `SELECT page FROM wikipedia WHERE channel = '#ar.wikipedia'`,
      );
    });

    it('removes comma (,) before ORDER', () => {
      const sql = `SELECT page FROM wikipedia WHERE channel = '#ar.wikipedia' GROUP BY 1, ORDER BY 1`;
      const suggestion = DruidError.getSuggestion(
        `Received an unexpected token [, ORDER] (line [1], column [70]), acceptable options:`,
      );
      expect(suggestion!.label).toEqual(`Remove comma (,) before ORDER`);
      expect(suggestion!.fn(sql)).toEqual(
        `SELECT page FROM wikipedia WHERE channel = '#ar.wikipedia' GROUP BY 1 ORDER BY 1`,
      );
    });

    it('removes trailing semicolon (;)', () => {
      const sql = `SELECT page FROM wikipedia WHERE channel = '#ar.wikipedia';`;
      const suggestion = DruidError.getSuggestion(
        `Received an unexpected token [;] (line [1], column [59]), acceptable options:`,
      );
      expect(suggestion!.label).toEqual(`Remove trailing semicolon (;)`);
      expect(suggestion!.fn(sql)).toEqual(
        `SELECT page FROM wikipedia WHERE channel = '#ar.wikipedia'`,
      );
    });

    it('does nothing there there is nothing to do', () => {
      const suggestion = DruidError.getSuggestion(
        `Encountered "channel" at line 1, column 35. Was expecting one of: <EOF> "EXCEPT" ...`,
      );
      expect(suggestion).toBeUndefined();
    });
  });

  describe('getDruidErrorMessage', () => {
    it('works with regular error response', () => {
      expect(
        getDruidErrorMessage({
          response: {
            data: {
              error: 'SQL parse failed',
              errorMessage: 'Encountered "<EOF>" at line 1, column 26.\nWas expecting one of:...',
              errorClass: 'org.apache.calcite.sql.parser.SqlParseException',
              host: null,
            },
          },
        }),
      ).toEqual(`SQL parse failed / Encountered "<EOF>" at line 1, column 26.
Was expecting one of:... / org.apache.calcite.sql.parser.SqlParseException`);
    });

    it('works with task error response', () => {
      expect(
        getDruidErrorMessage({
          response: {
            data: {
              taskId: '60a761ee-1ef5-437f-ae4c-adcc78c8a94c',
              state: 'FAILED',
              error: {
                error: 'SQL parse failed',
                errorMessage: 'Encountered "<EOF>" at line 1, column 26.\nWas expecting one of:...',
                errorClass: 'org.apache.calcite.sql.parser.SqlParseException',
                host: null,
              },
            },
          },
        }),
      ).toEqual(`SQL parse failed / Encountered "<EOF>" at line 1, column 26.
Was expecting one of:... / org.apache.calcite.sql.parser.SqlParseException`);
    });
  });
});
