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
  describe('DruidError', () => {
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
});
