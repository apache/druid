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
import { sane } from 'druid-query-toolkit/build/test-utils';
import React from 'react';

import { Capabilities } from '../../utils';

import { SegmentTimeline } from './segment-timeline';

jest.useFakeTimers('modern').setSystemTime(Date.parse('2021-06-08T12:34:56Z'));

describe('SegmentTimeline', () => {
  it('.getSqlQuery', () => {
    expect(
      SegmentTimeline.getSqlQuery(
        new Date('2020-01-01T00:00:00Z'),
        new Date('2021-02-01T00:00:00Z'),
      ),
    ).toEqual(sane`
      SELECT
        "start", "end", "datasource",
        COUNT(*) AS "count",
        SUM("size") AS "size"
      FROM sys.segments
      WHERE
        '2020-01-01T00:00:00.000Z' <= "start" AND
        "end" <= '2021-02-01T00:00:00.000Z' AND
        is_published = 1 AND
        is_overshadowed = 0
      GROUP BY 1, 2, 3
      ORDER BY "start" DESC
    `);
  });

  it('matches snapshot', () => {
    const segmentTimeline = <SegmentTimeline capabilities={Capabilities.FULL} />;
    const { container } = render(segmentTimeline);
    expect(container.firstChild).toMatchSnapshot();
  });
});
