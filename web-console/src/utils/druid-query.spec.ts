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

import { DruidError } from './druid-query';

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
      const suggestion = DruidError.getSuggestion(
        `Encountered "= =" at line 1, column 42. Was expecting one of: <EOF> "EXCEPT" ... "FETCH" ... "GROUP" ...`,
      );
      expect(suggestion!.label).toEqual(`Replace == with =`);
      expect(suggestion!.fn(`SELECT page FROM wikipedia WHERE channel == '#ar.wikipedia'`)).toEqual(
        `SELECT page FROM wikipedia WHERE channel = '#ar.wikipedia'`,
      );
    });

    it('works for incorrectly quoted literal', () => {
      const suggestion = DruidError.getSuggestion(
        `org.apache.calcite.runtime.CalciteContextException: From line 1, column 44 to line 1, column 58: Column '#ar.wikipedia' not found in any table`,
      );
      expect(suggestion!.label).toEqual(`Replace "#ar.wikipedia" with '#ar.wikipedia'`);
      expect(suggestion!.fn(`SELECT page FROM wikipedia WHERE channel = "#ar.wikipedia"`)).toEqual(
        `SELECT page FROM wikipedia WHERE channel = '#ar.wikipedia'`,
      );
    });

    it('removes comma (,) before FROM', () => {
      const suggestion = DruidError.getSuggestion(
        `Encountered "FROM" at line 1, column 14. Was expecting one of: "ABS" ...`,
      );
      expect(suggestion!.label).toEqual(`Remove , before FROM`);
      expect(suggestion!.fn(`SELECT page, FROM wikipedia WHERE channel = '#ar.wikipedia'`)).toEqual(
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
});
