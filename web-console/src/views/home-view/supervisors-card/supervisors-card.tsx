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

import { pluralIfNeeded, queryDruidSql, QueryManager } from '../../../utils';
import { Capabilities } from '../../../utils/capabilities';
import { HomeViewCard } from '../home-view-card/home-view-card';

export interface SupervisorsCardProps {
  capabilities: Capabilities;
}

export interface SupervisorsCardState {
  supervisorCountLoading: boolean;
  runningSupervisorCount: number;
  suspendedSupervisorCount: number;
  supervisorCountError?: string;
}

export class SupervisorsCard extends React.PureComponent<
  SupervisorsCardProps,
  SupervisorsCardState
> {
  private supervisorQueryManager: QueryManager<Capabilities, any>;

  constructor(props: SupervisorsCardProps, context: any) {
    super(props, context);
    this.state = {
      supervisorCountLoading: false,
      runningSupervisorCount: 0,
      suspendedSupervisorCount: 0,
    };

    this.supervisorQueryManager = new QueryManager({
      processQuery: async capabilities => {
        if (capabilities.hasSql()) {
          return (await queryDruidSql({
            query: `SELECT
  COUNT(*) FILTER (WHERE "suspended" = 0) AS "runningSupervisorCount",
  COUNT(*) FILTER (WHERE "suspended" = 1) AS "suspendedSupervisorCount"
FROM sys.supervisors`,
          }))[0];
        } else if (capabilities.hasOverlordAccess()) {
          const resp = await axios.get('/druid/indexer/v1/supervisor?full');
          const data = resp.data;
          const runningSupervisorCount = data.filter((d: any) => d.spec.suspended === false).length;
          const suspendedSupervisorCount = data.filter((d: any) => d.spec.suspended === true)
            .length;
          return {
            runningSupervisorCount,
            suspendedSupervisorCount,
          };
        } else {
          throw new Error(`must have SQL or overlord access`);
        }
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          runningSupervisorCount: result ? result.runningSupervisorCount : 0,
          suspendedSupervisorCount: result ? result.suspendedSupervisorCount : 0,
          supervisorCountLoading: loading,
          supervisorCountError: error,
        });
      },
    });
  }

  componentDidMount(): void {
    const { capabilities } = this.props;

    this.supervisorQueryManager.runQuery(capabilities);
  }

  componentWillUnmount(): void {
    this.supervisorQueryManager.terminate();
  }

  render(): JSX.Element {
    const {
      supervisorCountLoading,
      supervisorCountError,
      runningSupervisorCount,
      suspendedSupervisorCount,
    } = this.state;

    return (
      <HomeViewCard
        className="supervisors-card"
        href={'#ingestion'}
        icon={IconNames.LIST_COLUMNS}
        title={'Supervisors'}
        loading={supervisorCountLoading}
        error={supervisorCountError}
      >
        {!Boolean(runningSupervisorCount + suspendedSupervisorCount) && <p>No supervisors</p>}
        {Boolean(runningSupervisorCount) && (
          <p>{pluralIfNeeded(runningSupervisorCount, 'running supervisor')}</p>
        )}
        {Boolean(suspendedSupervisorCount) && (
          <p>{pluralIfNeeded(suspendedSupervisorCount, 'suspended supervisor')}</p>
        )}
      </HomeViewCard>
    );
  }
}
