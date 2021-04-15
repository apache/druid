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
import React from 'react';

import { useQueryManager } from '../../../hooks';
import { Api } from '../../../singletons';
import { pluralIfNeeded, queryDruidSql } from '../../../utils';
import { Capabilities } from '../../../utils';
import { HomeViewCard } from '../home-view-card/home-view-card';

export interface SupervisorCounts {
  running: number;
  suspended: number;
}

export interface SupervisorsCardProps {
  capabilities: Capabilities;
}

export const SupervisorsCard = React.memo(function SupervisorsCard(props: SupervisorsCardProps) {
  const [supervisorCountState] = useQueryManager<Capabilities, SupervisorCounts>({
    processQuery: async capabilities => {
      if (capabilities.hasSql()) {
        return (
          await queryDruidSql({
            query: `SELECT
  COUNT(*) FILTER (WHERE "suspended" = 0) AS "running",
  COUNT(*) FILTER (WHERE "suspended" = 1) AS "suspended"
FROM sys.supervisors`,
          })
        )[0];
      } else if (capabilities.hasOverlordAccess()) {
        const resp = await Api.instance.get('/druid/indexer/v1/supervisor?full');
        const data = resp.data;
        return {
          running: data.filter((d: any) => d.spec.suspended === false).length,
          suspended: data.filter((d: any) => d.spec.suspended === true).length,
        };
      } else {
        throw new Error(`must have SQL or overlord access`);
      }
    },
    initQuery: props.capabilities,
  });

  const { running, suspended } = supervisorCountState.data || {
    running: 0,
    suspended: 0,
  };

  return (
    <HomeViewCard
      className="supervisors-card"
      href={'#ingestion'}
      icon={IconNames.LIST_COLUMNS}
      title={'Supervisors'}
      loading={supervisorCountState.loading}
      error={supervisorCountState.error}
    >
      {!Boolean(running + suspended) && <p>No supervisors</p>}
      {Boolean(running) && <p>{pluralIfNeeded(running, 'running supervisor')}</p>}
      {Boolean(suspended) && <p>{pluralIfNeeded(suspended, 'suspended supervisor')}</p>}
    </HomeViewCard>
  );
});
