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
import { isLookupsUninitialized, pluralIfNeeded } from '../../../utils';
import { Capabilities } from '../../../utils/capabilities';
import { HomeViewCard } from '../home-view-card/home-view-card';

export interface LookupsCardProps {
  capabilities: Capabilities;
}

export const LookupsCard = React.memo(function LookupsCard(props: LookupsCardProps) {
  const [lookupsCountState] = useQueryManager<Capabilities, number>({
    processQuery: async capabilities => {
      if (capabilities.hasCoordinatorAccess()) {
        const resp = await axios.get('/druid/coordinator/v1/lookups/status');
        const data = resp.data;
        return sum(Object.keys(data).map(k => Object.keys(data[k]).length));
      } else {
        throw new Error(`must have coordinator access`);
      }
    },
    initQuery: props.capabilities,
  });

  return (
    <HomeViewCard
      className="lookups-card"
      href={'#lookups'}
      icon={IconNames.PROPERTIES}
      title={'Lookups'}
      loading={lookupsCountState.loading}
      error={!isLookupsUninitialized(lookupsCountState.error) ? lookupsCountState.error : undefined}
    >
      <p>
        {!isLookupsUninitialized(lookupsCountState.error)
          ? pluralIfNeeded(lookupsCountState.data || 0, 'lookup')
          : 'Lookups uninitialized'}
      </p>
    </HomeViewCard>
  );
});
