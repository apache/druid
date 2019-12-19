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

import { pluralIfNeeded, queryDruidSql, QueryManager } from '../../../utils';
import { Capabilities } from '../../../utils/capabilities';
import { deepGet } from '../../../utils/object-change';
import { HomeViewCard } from '../home-view-card/home-view-card';

export interface SegmentsCardProps {
  capabilities: Capabilities;
}

export interface SegmentsCardState {
  segmentCountLoading: boolean;
  segmentCount: number;
  unavailableSegmentCount: number;
  segmentCountError?: string;
}

export class SegmentsCard extends React.PureComponent<SegmentsCardProps, SegmentsCardState> {
  private segmentQueryManager: QueryManager<Capabilities, any>;

  constructor(props: SegmentsCardProps, context: any) {
    super(props, context);
    this.state = {
      segmentCountLoading: false,
      segmentCount: 0,
      unavailableSegmentCount: 0,
    };

    this.segmentQueryManager = new QueryManager({
      processQuery: async capabilities => {
        if (capabilities.hasSql()) {
          const segments = await queryDruidSql({
            query: `SELECT
  COUNT(*) as "count",
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
            count: availableSegmentNum + unavailableSegmentNum,
            unavailable: unavailableSegmentNum,
          };
        } else {
          throw new Error(`must have SQL or coordinator access`);
        }
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          segmentCountLoading: loading,
          segmentCount: result ? result.count : 0,
          unavailableSegmentCount: result ? result.unavailable : 0,
          segmentCountError: error,
        });
      },
    });
  }

  componentDidMount(): void {
    const { capabilities } = this.props;

    this.segmentQueryManager.runQuery(capabilities);
  }

  componentWillUnmount(): void {
    this.segmentQueryManager.terminate();
  }

  render(): JSX.Element {
    const {
      segmentCountLoading,
      segmentCountError,
      segmentCount,
      unavailableSegmentCount,
    } = this.state;

    return (
      <HomeViewCard
        className="segments-card"
        href={'#segments'}
        icon={IconNames.STACKED_CHART}
        title={'Segments'}
        loading={segmentCountLoading}
        error={segmentCountError}
      >
        <p>{pluralIfNeeded(segmentCount, 'segment')}</p>
        {Boolean(unavailableSegmentCount) && (
          <p>{pluralIfNeeded(unavailableSegmentCount, 'unavailable segment')}</p>
        )}
      </HomeViewCard>
    );
  }
}
