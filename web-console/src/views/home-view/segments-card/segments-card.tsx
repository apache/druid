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
import { sum } from 'd3-array';
import React from 'react';

import type { Capabilities } from '../../../helpers';
import { useQueryManager } from '../../../hooks';
import { Api } from '../../../singletons';
import { deepGet, pluralIfNeeded, queryDruidSql } from '../../../utils';
import { HomeViewCard } from '../home-view-card/home-view-card';

export interface SegmentCounts {
  active: number;
  cached_on_historical: number;
  unavailable: number;
  realtime: number;
}

export interface SegmentsCardProps {
  capabilities: Capabilities;
}

export const SegmentsCard = React.memo(function SegmentsCard(props: SegmentsCardProps) {
  const [segmentCountState] = useQueryManager<Capabilities, SegmentCounts>({
    initQuery: props.capabilities,
    processQuery: async capabilities => {
      if (capabilities.hasSql()) {
        const segments = await queryDruidSql({
          query: `SELECT
  COUNT(*) AS "active",
  COUNT(*) FILTER (WHERE is_available = 1) AS "cached_on_historical",
  COUNT(*) FILTER (WHERE is_available = 0 AND replication_factor > 0) AS "unavailable",
  COUNT(*) FILTER (WHERE is_realtime = 1) AS "realtime"
FROM sys.segments
WHERE is_active = 1`,
        });
        return segments.length === 1 ? segments[0] : null;
      } else if (capabilities.hasCoordinatorAccess()) {
        const loadstatusResp = await Api.instance.get('/druid/coordinator/v1/loadstatus?simple');
        const loadstatus = loadstatusResp.data;
        const unavailableSegmentNum = sum(Object.keys(loadstatus), key => loadstatus[key]);

        const datasourcesMetaResp = await Api.instance.get(
          '/druid/coordinator/v1/datasources?simple',
        );
        const datasourcesMeta = datasourcesMetaResp.data;
        const availableSegmentNum = sum(datasourcesMeta, (curr: any) =>
          deepGet(curr, 'properties.segments.count'),
        );

        return {
          active: availableSegmentNum + unavailableSegmentNum,
          cached_on_historical: availableSegmentNum,
          unavailable: unavailableSegmentNum, // This is no longer fully accurate because it does not replicate the [AND replication_factor > 0] condition of the SQL, this info is not in this API
          realtime: 0, // Realtime segments are sadly not reported by this API
        };
      } else {
        throw new Error(`must have SQL or coordinator access`);
      }
    },
  });

  const segmentCount: SegmentCounts = segmentCountState.data || {
    active: 0,
    cached_on_historical: 0,
    unavailable: 0,
    realtime: 0,
  };
  return (
    <HomeViewCard
      className="segments-card"
      href="#segments"
      icon={IconNames.STACKED_CHART}
      title="Segments"
      loading={segmentCountState.loading}
      error={segmentCountState.error}
    >
      <p>{pluralIfNeeded(segmentCount.active, 'active segment')}</p>
      {Boolean(segmentCount.unavailable) && (
        <p>
          {pluralIfNeeded(segmentCount.unavailable, 'segment')} waiting to be cached on historicals
        </p>
      )}
      <p>{pluralIfNeeded(segmentCount.cached_on_historical, 'segment')} cached on historicals</p>
      {Boolean(segmentCount.realtime) && (
        <p>{pluralIfNeeded(segmentCount.realtime, 'realtime segment')}</p>
      )}
    </HomeViewCard>
  );
});
