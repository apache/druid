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

import React from 'react';

import type { Capabilities } from '../../helpers';

import { DatasourcesCard } from './datasources-card/datasources-card';
import { LookupsCard } from './lookups-card/lookups-card';
import { SegmentsCard } from './segments-card/segments-card';
import { ServicesCard } from './services-card/services-card';
import { StatusCard } from './status-card/status-card';
import { SupervisorsCard } from './supervisors-card/supervisors-card';
import { TasksCard } from './tasks-card/tasks-card';

import './home-view.scss';

export interface HomeViewProps {
  capabilities: Capabilities;
}

export const HomeView = React.memo(function HomeView(props: HomeViewProps) {
  const { capabilities } = props;

  return (
    <div className="home-view app-view">
      <StatusCard capabilities={capabilities} />
      {capabilities.hasSqlOrCoordinatorAccess() && (
        <>
          <DatasourcesCard capabilities={capabilities} />
          <SegmentsCard capabilities={capabilities} />
        </>
      )}
      {capabilities.hasSqlOrOverlordAccess() && (
        <>
          <SupervisorsCard capabilities={capabilities} />
          <TasksCard capabilities={capabilities} />
        </>
      )}
      {capabilities.hasSqlOrCoordinatorAccess() && <ServicesCard capabilities={capabilities} />}
      {capabilities.hasCoordinatorAccess() && <LookupsCard capabilities={capabilities} />}
    </div>
  );
});
