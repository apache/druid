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

import { IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import { sum } from 'd3-array';
import React from 'react';

import { useQueryManager } from '../../../hooks';
import { Capabilities, deepGet, pluralIfNeeded, queryDruidSql } from '../../../utils';
import { HomeViewCard } from '../home-view-card/home-view-card';

export interface SegmentCounts {
  available: number;
  unavailable: number;
}

export interface SegmentsCardProps {
  capabilities: Capabilities;
}

export const SegmentsCard = React.memo(function SegmentsCard(props: SegmentsCardProps) {
  const [segmentCountState] = useQueryManager<Capabilities, SegmentCounts>({
    processQuery: async capabilities => {
      if (capabilities.hasSql()) {
        const segments = await queryDruidSql({
          query: `SELECT
  COUNT(*) as "available",
  COUNT(*) FILTER (WHERE is_available = 0) as "unavailable"
FROM sys.segments`,
        });
        return segments.length === 1 ? segments[0] : null;
      } else if (capabilities.hasCoordinatorAccess()) {
        const loadstatusResp = await axios.get('/druid/coordinator/v1/loadstatus?simple');
        const loadstatus = loadstatusResp.data;
        const unavailableSegmentNum = sum(Object.keys(loadstatus), key => loadstatus[key]);

        const datasourcesMetaResp = await axios.get('/druid/coordinator/v1/datasources?simple');
        const datasourcesMeta = datasourcesMetaResp.data;
        const availableSegmentNum = sum(datasourcesMeta, (curr: any) =>
          deepGet(curr, 'properties.segments.count'),
        );

        return {
          available: availableSegmentNum + unavailableSegmentNum,
          unavailable: unavailableSegmentNum,
        };
      } else {
        throw new Error(`must have SQL or coordinator access`);
      }
    },
    initQuery: props.capabilities,
  });

  const segmentCount = segmentCountState.data || { available: 0, unavailable: 0 };
  return (
    <HomeViewCard
      className="segments-card"
      href={'#segments'}
      icon={IconNames.STACKED_CHART}
      title={'Segments'}
      loading={segmentCountState.loading}
      error={segmentCountState.error}
    >
      <p>{pluralIfNeeded(segmentCount.available, 'segment')}</p>
      {Boolean(segmentCount.unavailable) && (
        <p>{pluralIfNeeded(segmentCount.unavailable, 'unavailable segment')}</p>
      )}
    </HomeViewCard>
  );
});
