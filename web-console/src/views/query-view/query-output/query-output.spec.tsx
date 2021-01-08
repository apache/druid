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

import { render } from '@testing-library/react';
import { QueryResult, SqlQuery } from 'druid-query-toolkit';
import React from 'react';

import { QueryOutput } from './query-output';

describe('query output', () => {
  it('matches snapshot', () => {
    const parsedQuery = SqlQuery.parse(`SELECT
  "language",
  COUNT(*) AS "Count", COUNT(DISTINCT "language") AS "dist_language", COUNT(*) FILTER (WHERE "language"= 'xxx') AS "language_filtered_count"
FROM "github"
WHERE "__time" >= CURRENT_TIMESTAMP - INTERVAL '1' DAY AND "language" != 'TypeScript'
GROUP BY 1
HAVING "Count" != 37392
ORDER BY "Count" DESC`);

    const queryOutput = (
      <QueryOutput
        runeMode={false}
        queryResult={QueryResult.fromRawResult(
          [
            ['language', 'Count', 'dist_language', 'language_filtered_count'],
            ['', 6881, 1, 0],
            ['JavaScript', 166, 1, 0],
            ['Python', 62, 1, 0],
            ['HTML', 46, 1, 0],
            [],
          ],
          false,
          true,
        ).attachQuery({}, parsedQuery)}
        onQueryChange={() => null}
      />
    );

    const { container } = render(queryOutput);
    expect(container.firstChild).toMatchSnapshot();
  });
});
