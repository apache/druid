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

export interface DatasourcesCardProps {
  capabilities: Capabilities;
}

export const DatasourcesCard = React.memo(function DatasourcesCard(props: DatasourcesCardProps) {
  const [datasourceCountState] = useQueryManager<Capabilities, number>({
    processQuery: async capabilities => {
      let datasources: string[];
      if (capabilities.hasSql()) {
        datasources = await queryDruidSql({
          query: `SELECT datasource FROM sys.segments GROUP BY 1`,
        });
      } else if (capabilities.hasCoordinatorAccess()) {
        const datasourcesResp = await Api.instance.get('/druid/coordinator/v1/datasources');
        datasources = datasourcesResp.data;
      } else {
        throw new Error(`must have SQL or coordinator access`);
      }

      return datasources.length;
    },
    initQuery: props.capabilities,
  });

  return (
    <HomeViewCard
      className="datasources-card"
      href={'#datasources'}
      icon={IconNames.MULTI_SELECT}
      title={'Datasources'}
      loading={datasourceCountState.loading}
      error={datasourceCountState.error}
    >
      <p>{pluralIfNeeded(datasourceCountState.data || 0, 'datasource')}</p>
    </HomeViewCard>
  );
});
