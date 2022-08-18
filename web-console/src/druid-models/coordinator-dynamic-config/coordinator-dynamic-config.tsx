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

import { Code } from '@blueprintjs/core';
import React from 'react';

import { Field } from '../../components';

export interface CoordinatorDynamicConfig {
  maxSegmentsToMove?: number;
  balancerComputeThreads?: number;
  emitBalancingStats?: boolean;
  killAllDataSources?: boolean;
  killDataSourceWhitelist?: string[];
  killPendingSegmentsSkipList?: string[];
  maxSegmentsInNodeLoadingQueue?: number;
  mergeBytesLimit?: number;
  mergeSegmentsLimit?: number;
  millisToWaitBeforeDeleting?: number;
  replicantLifetime?: number;
  replicationThrottleLimit?: number;
  decommissioningNodes?: string[];
  decommissioningMaxPercentOfMaxSegmentsToMove?: number;
  pauseCoordination?: boolean;
  maxNonPrimaryReplicantsToLoad?: number;
}

export const COORDINATOR_DYNAMIC_CONFIG_FIELDS: Field<CoordinatorDynamicConfig>[] = [
  {
    name: 'maxSegmentsToMove',
    type: 'number',
    defaultValue: 5,
    info: <>The maximum number of segments that can be moved at any given time.</>,
  },
  {
    name: 'balancerComputeThreads',
    type: 'number',
    defaultValue: 1,
    info: (
      <>
        Thread pool size for computing moving cost of segments in segment balancing. Consider
        increasing this if you have a lot of segments and moving segments starts to get stuck.
      </>
    ),
  },
  {
    name: 'emitBalancingStats',
    type: 'boolean',
    defaultValue: false,
    info: (
      <>
        Boolean flag for whether or not we should emit balancing stats. This is an expensive
        operation.
      </>
    ),
  },
  {
    name: 'killAllDataSources',
    type: 'boolean',
    defaultValue: false,
    info: (
      <>
        Send kill tasks for ALL dataSources if property <Code>druid.coordinator.kill.on</Code> is
        true. If this is set to true then <Code>killDataSourceWhitelist</Code> must not be specified
        or be empty list.
      </>
    ),
  },
  {
    name: 'killDataSourceWhitelist',
    type: 'string-array',
    emptyValue: [],
    info: (
      <>
        List of dataSources for which kill tasks are sent if property{' '}
        <Code>druid.coordinator.kill.on</Code> is true. This can be a list of comma-separated
        dataSources or a JSON array.
      </>
    ),
  },
  {
    name: 'killPendingSegmentsSkipList',
    type: 'string-array',
    emptyValue: [],
    info: (
      <>
        List of dataSources for which pendingSegments are NOT cleaned up if property{' '}
        <Code>druid.coordinator.kill.pendingSegments.on</Code> is true. This can be a list of
        comma-separated dataSources or a JSON array.
      </>
    ),
  },
  {
    name: 'maxSegmentsInNodeLoadingQueue',
    type: 'number',
    defaultValue: 0,
    info: (
      <>
        The maximum number of segments that could be queued for loading to any given server. This
        parameter could be used to speed up segments loading process, especially if there are
        &quot;slow&quot; nodes in the cluster (with low loading speed) or if too much segments
        scheduled to be replicated to some particular node (faster loading could be preferred to
        better segments distribution). Desired value depends on segments loading speed, acceptable
        replication time and number of nodes. Value 1000 could be a start point for a rather big
        cluster. Default value is 0 (loading queue is unbounded)
      </>
    ),
  },
  {
    name: 'mergeBytesLimit',
    type: 'size-bytes',
    defaultValue: 524288000,
    info: <>The maximum total uncompressed size in bytes of segments to merge.</>,
  },
  {
    name: 'mergeSegmentsLimit',
    type: 'number',
    defaultValue: 100,
    info: <>The maximum number of segments that can be in a single append task.</>,
  },
  {
    name: 'millisToWaitBeforeDeleting',
    type: 'number',
    defaultValue: 900000,
    info: (
      <>
        How long does the Coordinator need to be active before it can start removing (marking
        unused) segments in metadata storage.
      </>
    ),
  },
  {
    name: 'replicantLifetime',
    type: 'number',
    defaultValue: 15,
    info: (
      <>
        The maximum number of Coordinator runs for a segment to be replicated before we start
        alerting.
      </>
    ),
  },
  {
    name: 'replicationThrottleLimit',
    type: 'number',
    defaultValue: 10,
    info: <>The maximum number of segments that can be replicated at one time.</>,
  },
  {
    name: 'decommissioningNodes',
    type: 'string-array',
    emptyValue: [],
    info: (
      <>
        List of historical services to &apos;decommission&apos;. Coordinator will not assign new
        segments to &apos;decommissioning&apos; services, and segments will be moved away from them
        to be placed on non-decommissioning services at the maximum rate specified by{' '}
        <Code>decommissioningMaxPercentOfMaxSegmentsToMove</Code>.
      </>
    ),
  },
  {
    name: 'decommissioningMaxPercentOfMaxSegmentsToMove',
    type: 'number',
    defaultValue: 70,
    info: (
      <>
        The maximum number of segments that may be moved away from &apos;decommissioning&apos;
        services to non-decommissioning (that is, active) services during one Coordinator run. This
        value is relative to the total maximum segment movements allowed during one run which is
        determined by <Code>maxSegmentsToMove</Code>. If
        <Code>decommissioningMaxPercentOfMaxSegmentsToMove</Code> is 0, segments will neither be
        moved from or to &apos;decommissioning&apos; services, effectively putting them in a sort of
        &quot;maintenance&quot; mode that will not participate in balancing or assignment by load
        rules. Decommissioning can also become stalled if there are no available active services to
        place the segments. By leveraging the maximum percent of decommissioning segment movements,
        an operator can prevent active services from overload by prioritizing balancing, or decrease
        decommissioning time instead. The value should be between 0 and 100.
      </>
    ),
  },
  {
    name: 'useBatchedSegmentSampler',
    type: 'boolean',
    defaultValue: false,
    info: (
      <>
        Boolean flag for whether or not we should use the Reservoir Sampling with a reservoir of
        size k instead of fixed size 1 to pick segments to move. This option can be enabled to speed
        up segment balancing process, especially if there are huge number of segments in the cluster
        or if there are too many segments to move.
      </>
    ),
  },
  {
    name: 'percentOfSegmentsToConsiderPerMove',
    type: 'number',
    defaultValue: 100,
    info: (
      <>
        Deprecated. This will eventually be phased out by the batched segment sampler. You can
        enable the batched segment sampler now by setting the dynamic Coordinator config,
        useBatchedSegmentSampler, to true. Note that if you choose to enable the batched segment
        sampler, percentOfSegmentsToConsiderPerMove will no longer have any effect on balancing. If
        useBatchedSegmentSampler == false, this config defines the percentage of the total number of
        segments in the cluster that are considered every time a segment needs to be selected for a
        move. Druid orders servers by available capacity ascending (the least available capacity
        first) and then iterates over the servers. For each server, Druid iterates over the segments
        on the server, considering them for moving. The default config of 100% means that every
        segment on every server is a candidate to be moved. This should make sense for most small to
        medium-sized clusters. However, an admin may find it preferable to drop this value lower if
        they don&apos;t think that it is worthwhile to consider every single segment in the cluster
        each time it is looking for a segment to move.
      </>
    ),
  },
  {
    name: 'pauseCoordination',
    type: 'boolean',
    defaultValue: false,
    info: (
      <>
        Boolean flag for whether or not the coordinator should execute its various duties of
        coordinating the cluster. Setting this to true essentially pauses all coordination work
        while allowing the API to remain up.
      </>
    ),
  },
  {
    name: 'replicateAfterLoadTimeout',
    type: 'boolean',
    defaultValue: false,
    info: (
      <>
        Boolean flag for whether or not additional replication is needed for segments that have
        failed to load due to the expiry of coordinator load timeout. If this is set to true, the
        coordinator will attempt to replicate the failed segment on a different historical server.
      </>
    ),
  },
  {
    name: 'maxNonPrimaryReplicantsToLoad',
    type: 'number',
    defaultValue: 2147483647,
    info: (
      <>
        The maximum number of non-primary replicants to load in a single Coordinator cycle. Once
        this limit is hit, only primary replicants will be loaded for the remainder of the cycle.
        Tuning this value lower can help reduce the delay in loading primary segments when the
        cluster has a very large number of non-primary replicants to load (such as when a single
        historical drops out of the cluster leaving many under-replicated segments).
      </>
    ),
  },
];
