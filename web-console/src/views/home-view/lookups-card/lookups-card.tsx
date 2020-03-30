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

import { pluralIfNeeded, QueryManager } from '../../../utils';
import { Capabilities } from '../../../utils/capabilities';
import { HomeViewCard } from '../home-view-card/home-view-card';

export interface LookupsCardProps {
  capabilities: Capabilities;
}

export interface LookupsCardState {
  lookupsCountLoading: boolean;
  lookupsCount: number;
  lookupsUninitialized: boolean;
  lookupsCountError?: string;
}

export class LookupsCard extends React.PureComponent<LookupsCardProps, LookupsCardState> {
  private lookupsQueryManager: QueryManager<Capabilities, any>;

  constructor(props: LookupsCardProps, context: any) {
    super(props, context);
    this.state = {
      lookupsCountLoading: false,
      lookupsCount: 0,
      lookupsUninitialized: false,
    };

    this.lookupsQueryManager = new QueryManager({
      processQuery: async capabilities => {
        if (capabilities.hasCoordinatorAccess()) {
          const resp = await axios.get('/druid/coordinator/v1/lookups/status');
          const data = resp.data;
          const lookupsCount = sum(Object.keys(data).map(k => Object.keys(data[k]).length));
          return {
            lookupsCount,
          };
        } else {
          throw new Error(`must have coordinator access`);
        }
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          lookupsCount: result ? result.lookupsCount : 0,
          lookupsUninitialized: error === 'Request failed with status code 404',
          lookupsCountLoading: loading,
          lookupsCountError: error,
        });
      },
    });
  }

  componentDidMount(): void {
    const { capabilities } = this.props;
    this.lookupsQueryManager.runQuery(capabilities);
  }

  componentWillUnmount(): void {
    this.lookupsQueryManager.terminate();
  }

  render(): JSX.Element {
    const {
      lookupsCountLoading,
      lookupsCount,
      lookupsUninitialized,
      lookupsCountError,
    } = this.state;

    return (
      <HomeViewCard
        className="lookups-card"
        href={'#lookups'}
        icon={IconNames.PROPERTIES}
        title={'Lookups'}
        loading={lookupsCountLoading}
        error={!lookupsUninitialized ? lookupsCountError : undefined}
      >
        <p>
          {!lookupsUninitialized ? pluralIfNeeded(lookupsCount, 'lookup') : 'Lookups uninitialized'}
        </p>
      </HomeViewCard>
    );
  }
}
