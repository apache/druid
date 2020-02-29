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

import { PluralPairIfNeeded } from '../../../components/plural-pair-if-needed/plural-pair-if-needed';
import { lookupBy, queryDruidSql, QueryManager } from '../../../utils';
import { Capabilities } from '../../../utils/capabilities';
import { HomeViewCard } from '../home-view-card/home-view-card';

export interface ServicesCardProps {
  capabilities: Capabilities;
}

export interface ServicesCardState {
  serviceCountLoading: boolean;
  coordinatorCount: number;
  overlordCount: number;
  routerCount: number;
  brokerCount: number;
  historicalCount: number;
  middleManagerCount: number;
  peonCount: number;
  indexerCount: number;
  serviceCountError?: string;
}

export class ServicesCard extends React.PureComponent<ServicesCardProps, ServicesCardState> {
  private serviceQueryManager: QueryManager<Capabilities, any>;

  constructor(props: ServicesCardProps, context: any) {
    super(props, context);
    this.state = {
      serviceCountLoading: false,
      coordinatorCount: 0,
      overlordCount: 0,
      routerCount: 0,
      brokerCount: 0,
      historicalCount: 0,
      middleManagerCount: 0,
      peonCount: 0,
      indexerCount: 0,
    };

    this.serviceQueryManager = new QueryManager({
      processQuery: async capabilities => {
        if (capabilities.hasSql()) {
          const serviceCountsFromQuery: {
            service_type: string;
            count: number;
          }[] = await queryDruidSql({
            query: `SELECT server_type AS "service_type", COUNT(*) as "count" FROM sys.servers GROUP BY 1`,
          });
          return lookupBy(serviceCountsFromQuery, x => x.service_type, x => x.count);
        } else if (capabilities.hasCoordinatorAccess()) {
          const services = (await axios.get('/druid/coordinator/v1/servers?simple')).data;

          const middleManager = capabilities.hasOverlordAccess()
            ? (await axios.get('/druid/indexer/v1/workers')).data
            : [];

          return {
            historical: services.filter((s: any) => s.type === 'historical').length,
            middle_manager: middleManager.length,
            peon: services.filter((s: any) => s.type === 'indexer-executor').length,
          };
        } else {
          throw new Error(`must have SQL or coordinator access`);
        }
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          serviceCountLoading: loading,
          coordinatorCount: result ? result.coordinator : 0,
          overlordCount: result ? result.overlord : 0,
          routerCount: result ? result.router : 0,
          brokerCount: result ? result.broker : 0,
          historicalCount: result ? result.historical : 0,
          middleManagerCount: result ? result.middle_manager : 0,
          peonCount: result ? result.peon : 0,
          indexerCount: result ? result.indexer : 0,
          serviceCountError: error,
        });
      },
    });
  }

  componentDidMount(): void {
    const { capabilities } = this.props;

    this.serviceQueryManager.runQuery(capabilities);
  }

  componentWillUnmount(): void {
    this.serviceQueryManager.terminate();
  }

  render(): JSX.Element {
    const {
      serviceCountLoading,
      coordinatorCount,
      overlordCount,
      routerCount,
      brokerCount,
      historicalCount,
      middleManagerCount,
      peonCount,
      indexerCount,
      serviceCountError,
    } = this.state;
    return (
      <HomeViewCard
        className="services-card"
        href={'#services'}
        icon={IconNames.DATABASE}
        title={'Services'}
        loading={serviceCountLoading}
        error={serviceCountError}
      >
        <PluralPairIfNeeded
          firstCount={overlordCount}
          firstSingular="overlord"
          secondCount={coordinatorCount}
          secondSingular="coordinator"
        />
        <PluralPairIfNeeded
          firstCount={routerCount}
          firstSingular="router"
          secondCount={brokerCount}
          secondSingular="broker"
        />
        <PluralPairIfNeeded
          firstCount={historicalCount}
          firstSingular="historical"
          secondCount={middleManagerCount}
          secondSingular="middle manager"
        />
        <PluralPairIfNeeded
          firstCount={peonCount}
          firstSingular="peon"
          secondCount={indexerCount}
          secondSingular="indexer"
        />
      </HomeViewCard>
    );
  }
}
