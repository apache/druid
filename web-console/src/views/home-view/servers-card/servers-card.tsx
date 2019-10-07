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
import React from 'react';

import { compact, lookupBy, pluralIfNeeded, queryDruidSql, QueryManager } from '../../../utils';
import { HomeViewCard } from '../home-view-card/home-view-card';

export interface ServersCardProps {
  noSqlMode: boolean;
}

export interface ServersCardState {
  serverCountLoading: boolean;
  coordinatorCount: number;
  overlordCount: number;
  routerCount: number;
  brokerCount: number;
  historicalCount: number;
  middleManagerCount: number;
  peonCount: number;
  indexerCount: number;
  serverCountError?: string;
}

export class ServersCard extends React.PureComponent<ServersCardProps, ServersCardState> {
  static renderPluralIfNeededPair(
    count1: number,
    singular1: string,
    count2: number,
    singular2: string,
  ): JSX.Element | undefined {
    const text = compact([
      count1 ? pluralIfNeeded(count1, singular1) : undefined,
      count2 ? pluralIfNeeded(count2, singular2) : undefined,
    ]).join(', ');
    if (!text) return;
    return <p>{text}</p>;
  }

  private serverQueryManager: QueryManager<boolean, any>;

  constructor(props: ServersCardProps, context: any) {
    super(props, context);
    this.state = {
      serverCountLoading: false,
      coordinatorCount: 0,
      overlordCount: 0,
      routerCount: 0,
      brokerCount: 0,
      historicalCount: 0,
      middleManagerCount: 0,
      peonCount: 0,
      indexerCount: 0,
    };

    this.serverQueryManager = new QueryManager({
      processQuery: async noSqlMode => {
        if (noSqlMode) {
          const serversResp = await axios.get('/druid/coordinator/v1/servers?simple');
          const middleManagerResp = await axios.get('/druid/indexer/v1/workers');
          return {
            historical: serversResp.data.filter((s: any) => s.type === 'historical').length,
            middle_manager: middleManagerResp.data.length,
            peon: serversResp.data.filter((s: any) => s.type === 'indexer-executor').length,
          };
        } else {
          const serverCountsFromQuery: {
            server_type: string;
            count: number;
          }[] = await queryDruidSql({
            query: `SELECT server_type, COUNT(*) as "count" FROM sys.servers GROUP BY 1`,
          });
          return lookupBy(serverCountsFromQuery, x => x.server_type, x => x.count);
        }
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          serverCountLoading: loading,
          coordinatorCount: result ? result.coordinator : 0,
          overlordCount: result ? result.overlord : 0,
          routerCount: result ? result.router : 0,
          brokerCount: result ? result.broker : 0,
          historicalCount: result ? result.historical : 0,
          middleManagerCount: result ? result.middle_manager : 0,
          peonCount: result ? result.peon : 0,
          indexerCount: result ? result.indexer : 0,
          serverCountError: error,
        });
      },
    });
  }

  componentDidMount(): void {
    const { noSqlMode } = this.props;

    this.serverQueryManager.runQuery(noSqlMode);
  }

  componentWillUnmount(): void {
    this.serverQueryManager.terminate();
  }

  render(): JSX.Element {
    const {
      serverCountLoading,
      coordinatorCount,
      overlordCount,
      routerCount,
      brokerCount,
      historicalCount,
      middleManagerCount,
      peonCount,
      indexerCount,
      serverCountError,
    } = this.state;
    return (
      <HomeViewCard
        className="servers-card"
        href={'#servers'}
        icon={IconNames.DATABASE}
        title={'Servers'}
        loading={serverCountLoading}
        error={serverCountError}
      >
        {ServersCard.renderPluralIfNeededPair(
          overlordCount,
          'overlord',
          coordinatorCount,
          'coordinator',
        )}
        {ServersCard.renderPluralIfNeededPair(routerCount, 'router', brokerCount, 'broker')}
        {ServersCard.renderPluralIfNeededPair(
          historicalCount,
          'historical',
          middleManagerCount,
          'middle manager',
        )}
        {ServersCard.renderPluralIfNeededPair(peonCount, 'peon', indexerCount, 'indexer')}
      </HomeViewCard>
    );
  }
}
