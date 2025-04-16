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

import {
  partitionSetStatements,
  wrapInExplainAsParsedIfNeeded,
  wrapInExplainAsStringIfNeeded,
  wrapInExplainIfNeeded,
} from './explain';

describe('explain utils', () => {
  describe('partitionSetStatements', () => {
    it('works in simple case', () => {
      expect(
        partitionSetStatements(sane`
          -- A comment
          SET timeout = 100;
          SET timeout = 50;
          SELECT * FROM wikipedia
        `),
      ).toEqual([
        sane`
          -- A comment
          SET timeout = 100;
          SET timeout = 50;

        `,
        'SELECT * FROM wikipedia',
      ]);
    });

    it('only removes the first statements', () => {
      expect(
        partitionSetStatements(sane`
          SET timeout = 100;
          SELECT * FROM wikipedia


          SET timeout = 50;
          SET sqlTimeZone = 'Etc/UTC';
          SELECT * FROM wikipedia
        `),
      ).toEqual([
        'SET timeout = 100;\n',
        sane`
          SELECT * FROM wikipedia


          SET timeout = 50;
          SET sqlTimeZone = 'Etc/UTC';
          SELECT * FROM wikipedia
        `,
      ]);
    });
  });

  describe('wrapInExplain*', () => {
    const queries: [string, string][] = [
      [
        sane`
          SELECT *
          FROM (
            select * from wikipedia
          )
        `,
        sane`
          EXPLAIN PLAN FOR
          SELECT *
          FROM (
            select * from wikipedia
          )
        `,
      ],
      [
        sane`
          WITH "wikipedia" AS (select * from wikipedia2)
          SELECT *
          FROM (
            select * from wikipedia
          )
        `,
        sane`
          EXPLAIN PLAN FOR
          WITH "wikipedia" AS (select * from wikipedia2)
          SELECT *
          FROM (
            select * from wikipedia
          )
        `,
      ],
      [
        sane`
          INSERT INTO "table"
          SELECT *
          FROM (
            select * from wikipedia
          )
        `,
        sane`
          EXPLAIN PLAN FOR
          INSERT INTO "table"
          SELECT *
          FROM (
            select * from wikipedia
          )
        `,
      ],
      [
        sane`
          REPLACE INTO "table" OVERWRITE ALL
          SELECT *
          FROM (
            select * from wikipedia
          )
        `,
        sane`
          EXPLAIN PLAN FOR
          REPLACE INTO "table" OVERWRITE ALL
          SELECT *
          FROM (
            select * from wikipedia
          )
        `,
      ],
      [
        sane`
          REPLACE INTO "table" OVERWRITE ALL
          WITH "wikipedia" AS (select * from wikipedia2)
          SELECT *
          FROM (
            select * from wikipedia
          )
        `,
        sane`
          EXPLAIN PLAN FOR
          REPLACE INTO "table" OVERWRITE ALL
          WITH "wikipedia" AS (select * from wikipedia2)
          SELECT *
          FROM (
            select * from wikipedia
          )
        `,
      ],
    ];

    const sets = "-- A select comment\nSET a = 1;\nset b = 'EXPLAIN PLAN FOR';\n";

    it('works with queries', () => {
      for (const [before, after] of queries) {
        expect(wrapInExplainIfNeeded(before)).toEqual(after);
        expect(wrapInExplainAsParsedIfNeeded(before)).toEqual(after);
        expect(wrapInExplainAsStringIfNeeded(before)).toEqual(after);

        const beforeWithSets = sets + before;
        const afterWithSets = sets + after;
        expect(wrapInExplainIfNeeded(beforeWithSets)).toEqual(afterWithSets);
        expect(wrapInExplainAsParsedIfNeeded(beforeWithSets)).toEqual(afterWithSets);
        expect(wrapInExplainAsStringIfNeeded(beforeWithSets)).toEqual(afterWithSets);

        const beforeUnparsable = before + '~';
        const afterUnparsable = after + '~';
        expect(wrapInExplainIfNeeded(beforeUnparsable)).toEqual(afterUnparsable);
        expect(wrapInExplainAsStringIfNeeded(beforeUnparsable)).toEqual(afterUnparsable);

        const beforeUnparsableWithSets = beforeWithSets + '~';
        const afterUnparsableWithSets = afterWithSets + '~';
        expect(wrapInExplainIfNeeded(beforeUnparsableWithSets)).toEqual(afterUnparsableWithSets);
        expect(wrapInExplainAsStringIfNeeded(beforeUnparsableWithSets)).toEqual(
          afterUnparsableWithSets,
        );
      }
    });
  });
});
