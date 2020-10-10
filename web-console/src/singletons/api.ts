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

import axios from 'axios';

export const API_ENDPOINTS = {
  status: '/status',
  properties: '/status/properties',
  supervisor: '/druid/indexer/v1/supervisor',
  supervisorFull: '/druid/indexer/v1/supervisor?full',
  supervisorResumeAll: '/druid/indexer/v1/supervisor/resumeAll',
  supervisorSuspendAll: '/druid/indexer/v1/supervisor/suspendAll',
  supervisorTerminateAll: '/druid/indexer/v1/supervisor/terminateAll',
  task: '/druid/indexer/v1/task',
  tasks: '/druid/indexer/v1/tasks',
  worker: '/druid/indexer/v1/worker',
  workers: '/druid/indexer/v1/workers',
  datasources: '/druid/coordinator/v1/datasources',
  datasourcesSimple: '/druid/coordinator/v1/datasources?simple',
  overlordStatus: '/proxy/overlord/status',
  overlordStatusProperties: '/proxy/overlord/status/properties',
  coordinatorStatus: '/proxy/coordinator/status',
  coordinatorStatusProperties: '/proxy/coordinator/status/properties',
  coordinatorCompactionCompact: '/druid/coordinator/v1/compaction/compact',
  coordinatorRules: '/druid/coordinator/v1/rules',
  coordinatorLookupsConfig: '/druid/coordinator/v1/lookups/config',
  coordinatorLookupsConfigDiscover: '/druid/coordinator/v1/lookups/config?discover=true',
  coordinatorLookupsConfigAll: '/druid/coordinator/v1/lookups/config/all',
  coordinatorconfig: '/druid/coordinator/v1/config',
  coordinatorConfigCompaction: '/druid/coordinator/v1/config/compaction',
  coordinatorLoadStatusSimple: '/druid/coordinator/v1/loadstatus?simple',
  coordinatorMetadataDatasources: '/druid/coordinator/v1/metadata/datasources',
  coordinatorMetadataDatasourcesIncludeDisabled:
    '/druid/coordinator/v1/metadata/datasources?includeDisabled',
  coordinatorCompactionStatus: '/druid/coordinator/v1/compaction/status',
  coordinatorTiers: '/druid/coordinator/v1/tiers',
  coordinatorLookupsStatus: '/druid/coordinator/v1/lookups/status',
  coordinatorServersSimple: '/druid/coordinator/v1/servers?simple',
  coordinatorLoadqueueSimple: '/druid/coordinator/v1/loadqueue?simple',
  druidV2: '/druid/v2',
  druidV2Sql: '/druid/v2/sql',
};

function initialize() {
  const instance = axios.create();
  return instance;
}

export const Api = initialize();
