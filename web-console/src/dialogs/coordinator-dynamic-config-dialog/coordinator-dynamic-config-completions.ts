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

import type { JsonCompletionRule } from '../../utils';

export const COORDINATOR_DYNAMIC_CONFIG_COMPLETIONS: JsonCompletionRule[] = [
  // Root level properties
  {
    path: '$',
    isObject: true,
    completions: [
      {
        value: 'millisToWaitBeforeDeleting',
        documentation:
          'How long the Coordinator needs to be leader before marking overshadowed segments unused (default: 900000)',
      },
      {
        value: 'smartSegmentLoading',
        documentation:
          'Enable smart segment loading mode for optimized performance (default: true)',
      },
      {
        value: 'maxSegmentsToMove',
        documentation:
          'Maximum segments that can be moved in a Historical tier per run (default: 100)',
      },
      {
        value: 'balancerComputeThreads',
        documentation:
          'Thread pool size for computing segment balancing costs (default: num_cores / 2)',
      },
      {
        value: 'killDataSourceWhitelist',
        documentation: 'List of datasources for which kill tasks can be issued',
      },
      {
        value: 'killTaskSlotRatio',
        documentation: 'Ratio of total task slots allowed for kill tasks (default: 0.1)',
      },
      {
        value: 'maxKillTaskSlots',
        documentation: 'Maximum number of task slots for kill tasks (default: Integer.MAX_VALUE)',
      },
      {
        value: 'killPendingSegmentsSkipList',
        documentation: 'List of datasources to skip when cleaning pendingSegments',
      },
      {
        value: 'maxSegmentsInNodeLoadingQueue',
        documentation: 'Maximum segments allowed in any server load queue (default: 500)',
      },
      {
        value: 'replicantLifetime',
        documentation:
          'Maximum Coordinator runs a segment can wait in load queue before alert (default: 15)',
      },
      {
        value: 'replicationThrottleLimit',
        documentation: 'Maximum segment replicas assigned to a tier per run (default: 500)',
      },
      {
        value: 'decommissioningNodes',
        documentation: 'List of Historical servers to decommission',
      },
      {
        value: 'pauseCoordination',
        documentation: 'Pause all coordination duties while keeping API available (default: false)',
      },
      {
        value: 'replicateAfterLoadTimeout',
        documentation: 'Replicate segments that failed to load due to timeout (default: false)',
      },
      {
        value: 'useRoundRobinSegmentAssignment',
        documentation: 'Use round robin for segment assignment (default: true)',
      },
      {
        value: 'turboLoadingNodes',
        documentation: 'List of Historical servers to place in turbo loading mode (experimental)',
      },
    ],
  },
  // Properties only available when smartSegmentLoading is false
  {
    path: '$',
    isObject: true,
    condition: obj => obj.smartSegmentLoading === false,
    completions: [
      {
        value: 'maxSegmentsToMove',
        documentation: 'Maximum segments that can be moved in a Historical tier per run',
      },
      {
        value: 'maxSegmentsInNodeLoadingQueue',
        documentation: 'Maximum segments allowed in any server load queue',
      },
      {
        value: 'useRoundRobinSegmentAssignment',
        documentation: 'Use round robin for segment assignment',
      },
      {
        value: 'replicationThrottleLimit',
        documentation: 'Maximum segment replicas assigned to a tier per run',
      },
      {
        value: 'replicantLifetime',
        documentation: 'Maximum Coordinator runs a segment can wait in load queue before alert',
      },
    ],
  },
  // Boolean values
  {
    path: '$.smartSegmentLoading',
    completions: [
      { value: 'true', documentation: 'Enable smart segment loading (recommended)' },
      { value: 'false', documentation: 'Disable smart segment loading' },
    ],
  },
  {
    path: '$.pauseCoordination',
    completions: [
      { value: 'true', documentation: 'Pause coordination duties' },
      { value: 'false', documentation: 'Resume coordination duties (default)' },
    ],
  },
  {
    path: '$.replicateAfterLoadTimeout',
    completions: [
      { value: 'true', documentation: 'Enable replication after load timeout' },
      { value: 'false', documentation: 'Disable replication after load timeout (default)' },
    ],
  },
  {
    path: '$.useRoundRobinSegmentAssignment',
    completions: [
      { value: 'true', documentation: 'Use round robin assignment (default)' },
      { value: 'false', documentation: 'Use balancer strategy for assignment' },
    ],
  },
  // Numeric values
  {
    path: '$.millisToWaitBeforeDeleting',
    completions: [
      { value: '300000', documentation: '5 minutes' },
      { value: '600000', documentation: '10 minutes' },
      { value: '900000', documentation: '15 minutes (default)' },
      { value: '1800000', documentation: '30 minutes' },
      { value: '3600000', documentation: '1 hour' },
    ],
  },
  {
    path: '$.maxSegmentsToMove',
    completions: [
      { value: '10', documentation: '10 segments' },
      { value: '50', documentation: '50 segments' },
      { value: '100', documentation: '100 segments (default)' },
      { value: '200', documentation: '200 segments' },
      { value: '500', documentation: '500 segments' },
    ],
  },
  {
    path: '$.balancerComputeThreads',
    completions: [
      { value: '1', documentation: '1 thread' },
      { value: '2', documentation: '2 threads' },
      { value: '4', documentation: '4 threads' },
      { value: '8', documentation: '8 threads' },
      { value: '16', documentation: '16 threads' },
    ],
  },
  {
    path: '$.killTaskSlotRatio',
    completions: [
      { value: '0.05', documentation: '5% of slots' },
      { value: '0.1', documentation: '10% of slots (default)' },
      { value: '0.2', documentation: '20% of slots' },
      { value: '0.3', documentation: '30% of slots' },
      { value: '0.5', documentation: '50% of slots' },
      { value: '1.0', documentation: '100% of slots' },
    ],
  },
  {
    path: '$.maxKillTaskSlots',
    completions: [
      { value: '1', documentation: '1 task slot' },
      { value: '10', documentation: '10 task slots' },
      { value: '50', documentation: '50 task slots' },
      { value: '100', documentation: '100 task slots' },
      { value: '500', documentation: '500 task slots' },
      { value: '2147483647', documentation: 'No limit (default)' },
    ],
  },
  {
    path: '$.maxSegmentsInNodeLoadingQueue',
    completions: [
      { value: '100', documentation: '100 segments' },
      { value: '500', documentation: '500 segments (default)' },
      { value: '1000', documentation: '1000 segments' },
      { value: '5000', documentation: '5000 segments' },
    ],
  },
  {
    path: '$.replicantLifetime',
    completions: [
      { value: '5', documentation: '5 runs' },
      { value: '10', documentation: '10 runs' },
      { value: '15', documentation: '15 runs (default)' },
      { value: '30', documentation: '30 runs' },
      { value: '60', documentation: '60 runs' },
    ],
  },
  {
    path: '$.replicationThrottleLimit',
    completions: [
      { value: '100', documentation: '100 replicas' },
      { value: '200', documentation: '200 replicas' },
      { value: '500', documentation: '500 replicas (default)' },
      { value: '1000', documentation: '1000 replicas' },
    ],
  },
  // Array properties - suggest common examples
  {
    path: '$.killDataSourceWhitelist.[]',
    completions: [{ value: 'example_datasource', documentation: 'Example datasource name' }],
  },
  {
    path: '$.killPendingSegmentsSkipList.[]',
    completions: [{ value: 'example_datasource', documentation: 'Example datasource name' }],
  },
  {
    path: '$.decommissioningNodes.[]',
    completions: [{ value: 'historical-server:8083', documentation: 'Example historical server' }],
  },
  {
    path: '$.turboLoadingNodes.[]',
    completions: [{ value: 'historical-server:8083', documentation: 'Example historical server' }],
  },
];
