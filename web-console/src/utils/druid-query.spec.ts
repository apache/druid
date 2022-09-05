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

import { DruidError, getDruidErrorMessage, parseHtmlError } from './druid-query';

describe('DruidQuery', () => {
  describe('DruidError.parsePosition', () => {
    it('works for single error 1', () => {
      const message = `Encountered "COUNT" at line 2, column 12. Was expecting one of: <EOF> "AS" ... "EXCEPT" ... "FETCH" ... "FROM" ... "INTERSECT" ... "LIMIT" ...`;

      expect(DruidError.parsePosition(message)).toEqual({
        match: 'at line 2, column 12',
        row: 1,
        column: 11,
      });
    });

    it('works for single error 2', () => {
      const message = `org.apache.calcite.runtime.CalciteContextException: At line 2, column 20: Unknown identifier '*'`;

      expect(DruidError.parsePosition(message)).toEqual({
        match: 'At line 2, column 20',
        row: 1,
        column: 19,
      });
    });

    it('works for range', () => {
      const message = `org.apache.calcite.runtime.CalciteContextException: From line 2, column 13 to line 2, column 25: No match found for function signature SUMP(<NUMERIC>)`;

      expect(DruidError.parsePosition(message)).toEqual({
        match: 'From line 2, column 13 to line 2, column 25',
        row: 1,
        column: 12,
        endRow: 1,
        endColumn: 25,
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
      const suggestion = DruidError.getSuggestion(`Encountered "= =" at line 3, column 15.`);
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
        `Encountered "= =" at line 4, column 15. Was expecting one of: <EOF> "EXCEPT" ... "FETCH" ... "GROUP" ...`,
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
        `org.apache.calcite.runtime.CalciteContextException: From line 3, column 17 to line 3, column 31: Column '#ar.wikipedia' not found in any table`,
      );
      expect(suggestion!.label).toEqual(`Replace "#ar.wikipedia" with '#ar.wikipedia'`);
      expect(suggestion!.fn(sql)).toEqual(sane`
        SELECT *
        FROM wikipedia -- test "#ar.wikipedia"
        WHERE channel = '#ar.wikipedia'
      `);
    });

    it('works for incorrectly quoted AS alias', () => {
      const suggestion = DruidError.getSuggestion(`Encountered "AS \\'c\\'" at line 1, column 16.`);
      expect(suggestion!.label).toEqual(`Replace 'c' with "c"`);
      expect(suggestion!.fn(`SELECT channel AS 'c' FROM wikipedia`)).toEqual(
        `SELECT channel AS "c" FROM wikipedia`,
      );
    });

    it('removes comma (,) before FROM', () => {
      const suggestion = DruidError.getSuggestion(
        `Encountered ", FROM" at line 1, column 12. Was expecting one of: "ABS" ...`,
      );
      expect(suggestion!.label).toEqual(`Remove , before FROM`);
      expect(suggestion!.fn(`SELECT page, FROM wikipedia WHERE channel = '#ar.wikipedia'`)).toEqual(
        `SELECT page FROM wikipedia WHERE channel = '#ar.wikipedia'`,
      );
    });

    it('removes comma (,) before ORDER', () => {
      const suggestion = DruidError.getSuggestion(
        `Encountered ", ORDER" at line 1, column 14. Was expecting one of: "ABS" ...`,
      );
      expect(suggestion!.label).toEqual(`Remove , before ORDER`);
      expect(
        suggestion!.fn(
          `SELECT page FROM wikipedia WHERE channel = '#ar.wikipedia' GROUP BY 1, ORDER BY 1`,
        ),
      ).toEqual(`SELECT page FROM wikipedia WHERE channel = '#ar.wikipedia' GROUP BY 1 ORDER BY 1`);
    });

    it('removes trailing semicolon (;)', () => {
      const suggestion = DruidError.getSuggestion(
        `Encountered ";" at line 1, column 59. Was expecting one of: "ABS" ...`,
      );
      expect(suggestion!.label).toEqual(`Remove trailing ;`);
      expect(suggestion!.fn(`SELECT page FROM wikipedia WHERE channel = '#ar.wikipedia';`)).toEqual(
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

  describe('misc', () => {
    it('parseHtmlError', () => {
      expect(parseHtmlError('<div></div>')).toMatchInlineSnapshot(`undefined`);
    });

    it('parseHtmlError', () => {
      expect(getDruidErrorMessage({})).toMatchInlineSnapshot(`undefined`);
    });
  });
});
